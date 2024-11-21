#!/usr/bin/env python3
# To run this script you must install docker and piddeptree python package
#

import os
import subprocess
import sys


def build_docker_deps(image_name: str, imagedir: str) -> None:
    print("Fetch the newest manifest for", image_name)
    pip_cmd = (
        "pip install pipdeptree 2>/dev/null 1>/dev/null && pipdeptree --freeze "
        "--warn silence --exclude pipdeptree"
    )
    # /=/!d - remove dependencies without pin
    # ubuntu - ignore system packages
    # \s - remove spaces
    sed = r"sed '/==/!d; /==.*+ubuntu/d; s/\s//g'"
    cmd = rf"""docker run --rm --entrypoint "/bin/bash" {image_name} -c "{pip_cmd} | {sed} | sort -u" > {imagedir}/requirements.txt"""
    print("Running the command:", cmd)
    subprocess.check_call(cmd, shell=True)


def check_docker_file_install_with_pip(filepath):
    image_name = None
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if "docker build" in line:
                arr = line.split(" ")
                if len(arr) > 4:
                    image_name = arr[4]
            if "pip3 install" in line or "pip install" in line:
                return image_name, True
    return image_name, False


def process_affected_images(images_dir: str) -> None:
    for root, _dirs, files in os.walk(images_dir):
        for f in files:
            if f == "Dockerfile":
                docker_file_path = os.path.join(root, f)
                print("Checking image on path", docker_file_path)
                image_name, has_pip = check_docker_file_install_with_pip(
                    docker_file_path
                )
                if has_pip:
                    print("Find pip in", image_name)
                    try:
                        build_docker_deps(image_name, root)
                    except Exception as ex:
                        print(ex)
                else:
                    print("Pip not found in", docker_file_path)


process_affected_images(sys.argv[1])
