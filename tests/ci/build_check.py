#!/usr/bin/env python3
#
import subprocess
import logging
import json
import os
import sys
import time
from github import Github
from s3_helper import S3Helper
from pr_info import PRInfo
from get_robot_token import get_best_robot_token
from version_helper import get_version_from_repo, update_version_local


def get_build_config(build_check_name, build_number, repo_path):
    if build_check_name == 'ClickHouse build check (actions)':
        build_config_name = 'build_config'
    elif build_check_name == 'ClickHouse special build check (actions)':
        build_config_name = 'special_build_config'
    else:
        raise Exception(f"Unknown build check name {build_check_name}")

    ci_config_path = os.path.join(repo_path, "tests/ci/ci_config.json")
    with open(ci_config_path, 'r') as ci_config:
        config_dict = json.load(ci_config)
        return config_dict[build_config_name][build_number]


def _can_export_binaries(build_config):
    if build_config['package-type'] != 'deb':
        return False
    if build_config['bundled'] != "bundled":
        return False
    if build_config['splitted'] == 'splitted':
        return False
    if build_config['sanitizer'] != '':
        return True
    if build_config['build-type'] != '':
        return True
    return False


def get_packager_cmd(build_config, packager_path, output_path, build_version, image_version, ccache_path):
    package_type = build_config['package-type']
    comp = build_config['compiler']
    cmd = f"cd {packager_path} && ./packager --output-dir={output_path} --package-type={package_type} --compiler={comp}"

    if build_config['build-type']:
        cmd += ' --build-type={}'.format(build_config['build-type'])
    if build_config['sanitizer']:
        cmd += ' --sanitizer={}'.format(build_config['sanitizer'])
    if build_config['bundled'] == 'unbundled':
        cmd += ' --unbundled'
    if build_config['splitted'] == 'splitted':
        cmd += ' --split-binary'
    if build_config['tidy'] == 'enable':
        cmd += ' --clang-tidy'

    cmd += ' --cache=ccache'
    cmd += ' --ccache_dir={}'.format(ccache_path)

    if 'alien_pkgs' in build_config and build_config['alien_pkgs']:
        cmd += ' --alien-pkgs'

    cmd += ' --docker-image-version={}'.format(image_version)
    cmd += ' --version={}'.format(build_version)

    if _can_export_binaries(build_config):
        cmd += ' --with-binaries=tests'

    return cmd

def get_image_name(build_config):
    if build_config['bundled'] != 'bundled':
        return 'clickhouse/unbundled-builder'
    elif build_config['package-type'] != 'deb':
        return 'clickhouse/binary-builder'
    else:
        return 'clickhouse/deb-builder'


def build_clickhouse(packager_cmd, logs_path):
    build_log_path = os.path.join(logs_path, 'build_log.log')
    with open(build_log_path, 'w') as log_file:
        retcode = subprocess.Popen(packager_cmd, shell=True, stderr=log_file, stdout=log_file).wait()
        if retcode == 0:
            logging.info("Built successfully")
        else:
            logging.info("Build failed")
    return build_log_path, retcode == 0

def build_config_to_string(build_config):
    if build_config["package-type"] == "performance":
        return "performance"

    return "_".join([
        build_config['compiler'],
        build_config['build-type'] if build_config['build-type'] else "relwithdebuginfo",
        build_config['sanitizer'] if build_config['sanitizer'] else "none",
        build_config['bundled'],
        build_config['splitted'],
        "tidy" if build_config['tidy'] == "enable" else "notidy",
        "with_coverage" if build_config['with_coverage'] else "without_coverage",
        build_config['package-type'],
    ])

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo_path = os.getenv("REPO_COPY", os.path.abspath("../../"))
    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))
    caches_path = os.getenv("CACHES_PATH", temp_path)

    build_check_name = sys.argv[1]
    build_number = int(sys.argv[2])

    build_config = get_build_config(build_check_name, build_number, repo_path)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    with open(os.getenv('GITHUB_EVENT_PATH'), 'r') as event_file:
        event = json.load(event_file)

    pr_info = PRInfo(event)

    logging.info("Repo copy path %s", repo_path)

    gh = Github(get_best_robot_token())

    images_path = os.path.join(os.getenv("IMAGES_PATH", temp_path), 'changed_images.json')
    image_name = get_image_name(build_config)
    image_version = 'latest'
    if os.path.exists(images_path):
        logging.info("Images file exists")
        with open(images_path, 'r') as images_fd:
            images = json.load(images_fd)
            logging.info("Got images %s", images)
            if image_name in images:
                image_version = images[image_name]

    for i in range(10):
        try:
            logging.info("Pulling image %s:%s", image_name, image_version)
            subprocess.check_output(f"docker pull {image_name}:{image_version}", stderr=subprocess.STDOUT, shell=True)
            break
        except Exception as ex:
            time.sleep(i * 3)
            logging.info("Got execption pulling docker %s", ex)
    else:
        raise Exception(f"Cannot pull dockerhub for image docker pull {image_name}:{image_version}")

    version = get_version_from_repo(repo_path)
    version.tweak_update()
    update_version_local(repo_path, pr_info.sha, version)

    build_name = build_config_to_string(build_config)
    logging.info("Build short name %s", build_name)
    subprocess.check_call(f"echo 'BUILD_NAME=build_urls_{build_name}' >> $GITHUB_ENV", shell=True)

    build_output_path = os.path.join(temp_path, build_name)
    if not os.path.exists(build_output_path):
        os.makedirs(build_output_path)

    ccache_path = os.path.join(caches_path, build_name + '_ccache')
    if not os.path.exists(ccache_path):
        os.makedirs(ccache_path)

    packager_cmd = get_packager_cmd(build_config, os.path.join(repo_path, "docker/packager"), build_output_path, version.get_version_string(), image_version, ccache_path)
    logging.info("Going to run packager with %s", packager_cmd)

    build_clickhouse_log = os.path.join(temp_path, "build_log")
    if not os.path.exists(build_clickhouse_log):
        os.makedirs(build_clickhouse_log)

    start = time.time()
    log_path, success = build_clickhouse(packager_cmd, build_clickhouse_log)
    elapsed = int(time.time() - start)
    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {build_output_path}", shell=True)
    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {ccache_path}", shell=True)
    logging.info("Build finished with %s, log path %s", success, log_path)

    s3_helper = S3Helper('https://s3.amazonaws.com')
    s3_path_prefix = str(pr_info.number) + "/" + pr_info.sha + "/" + build_check_name.lower().replace(' ', '_') + "/" + build_name
    if os.path.exists(log_path):
        log_url = s3_helper.upload_build_file_to_s3(log_path, s3_path_prefix + "/" + os.path.basename(log_path))
        logging.info("Log url %s", log_url)
    else:
        logging.info("Build log doesn't exist")

    build_urls = s3_helper.upload_build_folder_to_s3(build_output_path, s3_path_prefix, keep_dirs_in_s3_path=False, upload_symlinks=False)
    logging.info("Got build URLs %s", build_urls)

    print("::notice ::Build URLs: {}".format('\n'.join(build_urls)))

    result = {
        "log_url": log_url,
        "build_urls": build_urls,
        "build_config": build_config,
        "elapsed_seconds": elapsed,
        "status": success,
    }

    print("::notice ::Log URL: {}".format(log_url))

    with open(os.path.join(temp_path, "build_urls_" + build_name + '.json'), 'w') as build_links:
        json.dump(result, build_links)
