import os
from ftplib import FTP
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import glob

# 下载函数
def download_file(args):
    ftp_link, local_dir = args  # 解包参数
    ftp_link = ftp_link.strip()  # 去除多余的空格和换行符
    url_split = ftp_link.split('/')
    ftp_host = url_split[2].split('@')[-1]  # FTP主机地址
    ftp_path = '/'.join(url_split[3:])  # FTP路径
    ftp_file = url_split[-1]  # 要下载的文件名
    ftp_credentials = url_split[2].split('@')[0].split(':')  # FTP凭据（用户名和密码）

    local_file_path = os.path.join(local_dir, ftp_file)  # 本地文件路径

    # 检查是否已经存在相同的文件
    if os.path.exists(local_file_path):
        existing_file_size = os.path.getsize(local_file_path)  # 已存在文件的大小
        ftp = FTP(ftp_host)
        ftp.login(user=ftp_credentials[0], passwd=ftp_credentials[1])
        ftp.voidcmd('TYPE I')  # 设置为二进制模式
        remote_file_size = ftp.size(ftp_path)  # 远程文件的大小
        ftp.quit()  # 退出FTP连接
        if existing_file_size >= remote_file_size:
            tqdm.write(f"Skipped {ftp_file}.")  # 文件被跳过时的输出
            return f"Skipped {ftp_file}."

    retry_count = 0  # 初始化重试计数器
    while retry_count < 10:  # 重试次数限制为10次
        try:
            ftp = FTP(ftp_host)
            ftp.login(user=ftp_credentials[0], passwd=ftp_credentials[1])
            with open(local_file_path, 'wb') as local_file:
                ftp.retrbinary('RETR ' + ftp_path, local_file.write)  # 下载文件
            ftp.quit()  # 退出FTP连接
            tqdm.write(f"Downloaded {ftp_file}.")  # 文件下载成功时的输出
            return f"Downloaded {ftp_file}."
        except:
            retry_count += 1  # 增加重试计数
    tqdm.write(f"Failed to download {ftp_file} after 10 attempts.")  # 文件下载失败时的输出
    return f"Failed {ftp_file}."

local_dir = "D:\\FY4data"
if not os.path.exists(local_dir):
    os.makedirs(local_dir)
#txt_files = ['A202310260534115389.txt', 'A202310260339791289.txt', 'A202310260110661586.txt', 'A202310260515906770.txt']

# 获取指定目录下所有符合条件的TXT文件
txt_files = glob.glob("D:\\A20240801*.txt")

def main():
    global_failed_links = []
    for txt_file in txt_files:
        failed_links = []
        with open(txt_file, 'r') as f:
            ftp_links = f.readlines()
        args = [(ftp_link, local_dir) for ftp_link in ftp_links]
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(tqdm(executor.map(download_file, args), total=len(ftp_links)))
            for result in results:
                if "Failed" in result:
                    failed_links.append(args[results.index(result)][0])
                tqdm.write(result)
        tqdm.write(f"Finished downloading files from {txt_file}")
        global_failed_links.extend(failed_links)

    retry_count = 0
    while global_failed_links and retry_count < 100:
        retry_count += 1
        tqdm.write(f"Retrying failed links, attempt {retry_count}...")
        failed_links = []
        args = [(ftp_link, local_dir) for ftp_link in global_failed_links]
        with ThreadPoolExecutor() as executor:
            results = list(tqdm(executor.map(download_file, args), total=len(global_failed_links)))
            for result in results:
                if "Failed" in result:
                    failed_links.append(args[results.index(result)][0])
                tqdm.write(result)
        global_failed_links = failed_links

    if global_failed_links:
        with open('failed_links.txt', 'w') as f:
            for link in global_failed_links:
                f.write(f"{link}\n")
        tqdm.write("Failed to download some files after 100 attempts. See failed_links.txt for details.")

if __name__ == '__main__':
    main()
