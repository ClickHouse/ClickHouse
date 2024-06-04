#!/usr/bin/env python3
import sys
import os
import logging
from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from s3_helper import S3Helper
from pr_info import PRInfo
from build_download_helper import download_builds_filter
import hashlib
from pathlib import Path

GPG_BINARY_SIGNING_KEY = os.getenv("GPG_BINARY_SIGNING_KEY")
GPG_BINARY_SIGNING_PASSPHRASE = os.getenv("GPG_BINARY_SIGNING_PASSPHRASE")

CHECK_NAME = "Sign release (actions)"

def hash_file(file_path):
    BLOCK_SIZE = 65536 # The size of each read from the file

    file_hash = hashlib.sha256() # Create the hash object, can use something other than `.sha256()` if you wish
    with open(file_path, 'rb') as f: # Open the file to read it's bytes
        fb = f.read(BLOCK_SIZE) # Read from the file. Take in the amount declared above
        while len(fb) > 0: # While there is still data being read from the file
            file_hash.update(fb) # Update the hash
            fb = f.read(BLOCK_SIZE) # Read the next block from the file

    hash_file_path = file_path + '.sha256'
    with open(hash_file_path, 'x') as f:
        digest = file_hash.hexdigest()
        f.write(digest)
        print(f'Hashed {file_path}: {digest}')

    return hash_file_path

def sign_file(file_path):
    priv_key_file_path = 'priv.key'
    with open(priv_key_file_path, 'x') as f:
        f.write(GPG_BINARY_SIGNING_KEY)

    out_file_path = f'{file_path}.gpg'

    os.system(f'echo {GPG_BINARY_SIGNING_PASSPHRASE} | gpg --batch --import {priv_key_file_path}')
    os.system(f'gpg -o {out_file_path} --pinentry-mode=loopback --batch --yes --passphrase {GPG_BINARY_SIGNING_PASSPHRASE} --sign {file_path}')
    print(f"Signed {file_path}")
    os.remove(priv_key_file_path)

    return out_file_path

def main():
    reports_path = REPORTS_PATH

    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)

    pr_info = PRInfo()

    logging.info("Repo copy path %s", REPO_COPY)

    s3_helper = S3Helper()

    s3_path_prefix = Path(f"{pr_info.number}/{pr_info.sha}/" + CHECK_NAME.lower().replace(
        " ", "_"
    ).replace("(", "_").replace(")", "_").replace(",", "_"))

    # downloads `package_release` artifacts generated
    download_builds_filter(CHECK_NAME, reports_path, TEMP_PATH)

    for f in os.listdir(TEMP_PATH):
        full_path = os.path.join(TEMP_PATH, f)
        hashed_file_path = hash_file(full_path)
        signed_file_path = sign_file(hashed_file_path)
        s3_path = s3_path_prefix / os.path.basename(signed_file_path)
        s3_helper.upload_build_file_to_s3(Path(signed_file_path), str(s3_path))
        print(f'Uploaded file {signed_file_path} to {s3_path}')

    # Signed hashes are:
    # clickhouse-client_22.3.15.2.altinitystable_amd64.deb.sha512.gpg              clickhouse-keeper_22.3.15.2.altinitystable_x86_64.apk.sha512.gpg
    # clickhouse-client-22.3.15.2.altinitystable-amd64.tgz.sha512.gpg              clickhouse-keeper-22.3.15.2.altinitystable.x86_64.rpm.sha512.gpg
    # clickhouse-client_22.3.15.2.altinitystable_x86_64.apk.sha512.gpg             clickhouse-keeper-dbg_22.3.15.2.altinitystable_amd64.deb.sha512.gpg
    # clickhouse-client-22.3.15.2.altinitystable.x86_64.rpm.sha512.gpg             clickhouse-keeper-dbg-22.3.15.2.altinitystable-amd64.tgz.sha512.gpg
    # clickhouse-common-static_22.3.15.2.altinitystable_amd64.deb.sha512.gpg       clickhouse-keeper-dbg_22.3.15.2.altinitystable_x86_64.apk.sha512.gpg
    # clickhouse-common-static-22.3.15.2.altinitystable-amd64.tgz.sha512.gpg       clickhouse-keeper-dbg-22.3.15.2.altinitystable.x86_64.rpm.sha512.gpg
    # clickhouse-common-static_22.3.15.2.altinitystable_x86_64.apk.sha512.gpg      clickhouse-keeper.sha512.gpg
    # clickhouse-common-static-22.3.15.2.altinitystable.x86_64.rpm.sha512.gpg      clickhouse-library-bridge.sha512.gpg
    # clickhouse-common-static-dbg_22.3.15.2.altinitystable_amd64.deb.sha512.gpg   clickhouse-odbc-bridge.sha512.gpg
    # clickhouse-common-static-dbg-22.3.15.2.altinitystable-amd64.tgz.sha512.gpg   clickhouse-server_22.3.15.2.altinitystable_amd64.deb.sha512.gpg
    # clickhouse-common-static-dbg_22.3.15.2.altinitystable_x86_64.apk.sha512.gpg  clickhouse-server-22.3.15.2.altinitystable-amd64.tgz.sha512.gpg
    # clickhouse-common-static-dbg-22.3.15.2.altinitystable.x86_64.rpm.sha512.gpg  clickhouse-server_22.3.15.2.altinitystable_x86_64.apk.sha512.gpg
    # clickhouse-keeper_22.3.15.2.altinitystable_amd64.deb.sha512.gpg              clickhouse-server-22.3.15.2.altinitystable.x86_64.rpm.sha512.gpg
    # clickhouse-keeper-22.3.15.2.altinitystable-amd64.tgz.sha512.gpg              clickhouse.sha512.gpg

    sys.exit(0)

if __name__ == "__main__":
    main()
