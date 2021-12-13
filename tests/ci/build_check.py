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
from pr_info import PRInfo, get_event
from get_robot_token import get_best_robot_token
from version_helper import get_version_from_repo, update_version_local
from ccache_utils import get_ccache_if_not_exists, upload_ccache
from ci_config import CI_CONFIG
from docker_pull_helper import get_image_with_version
from tee_popen import TeePopen


def get_build_config(build_check_name, build_name):
    if build_check_name == 'ClickHouse build check (actions)':
        build_config_name = 'build_config'
    else:
        raise Exception(f"Unknown build check name {build_check_name}")

    return CI_CONFIG[build_config_name][build_name]


def _can_export_binaries(build_config):
    if build_config['package_type'] != 'deb':
        return False
    if build_config['bundled'] != "bundled":
        return False
    if build_config['splitted'] == 'splitted':
        return False
    if build_config['sanitizer'] != '':
        return True
    if build_config['build_type'] != '':
        return True
    return False


def get_packager_cmd(build_config, packager_path, output_path, build_version, image_version, ccache_path, pr_info):
    package_type = build_config['package_type']
    comp = build_config['compiler']
    cmd = f"cd {packager_path} && ./packager --output-dir={output_path} --package-type={package_type} --compiler={comp}"

    if build_config['build_type']:
        cmd += ' --build-type={}'.format(build_config['build_type'])
    if build_config['sanitizer']:
        cmd += ' --sanitizer={}'.format(build_config['sanitizer'])
    if build_config['splitted'] == 'splitted':
        cmd += ' --split-binary'
    if build_config['tidy'] == 'enable':
        cmd += ' --clang-tidy'

    cmd += ' --cache=ccache'
    cmd += ' --ccache_dir={}'.format(ccache_path)

    if 'alien_pkgs' in build_config and build_config['alien_pkgs']:
        if pr_info.number == 0 or 'release' in pr_info.labels:
            cmd += ' --alien-pkgs rpm tgz'

    cmd += ' --docker-image-version={}'.format(image_version)
    cmd += ' --version={}'.format(build_version)

    if _can_export_binaries(build_config):
        cmd += ' --with-binaries=tests'

    return cmd

def get_image_name(build_config):
    if build_config['package_type'] != 'deb':
        return 'clickhouse/binary-builder'
    else:
        return 'clickhouse/deb-builder'


def build_clickhouse(packager_cmd, logs_path):
    build_log_path = os.path.join(logs_path, 'build_log.log')
    with TeePopen(packager_cmd, build_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Built successfully")
        else:
            logging.info("Build failed")
    return build_log_path, retcode == 0


def get_build_results_if_exists(s3_helper, s3_prefix):
    try:
        content = s3_helper.list_prefix(s3_prefix)
        return content
    except Exception as ex:
        logging.info("Got exception %s listing %s", ex, s3_prefix)
        return None

def create_json_artifact(temp_path, build_name, log_url, build_urls, build_config, elapsed, success):
    subprocess.check_call(f"echo 'BUILD_NAME=build_urls_{build_name}' >> $GITHUB_ENV", shell=True)

    result = {
        "log_url": log_url,
        "build_urls": build_urls,
        "build_config": build_config,
        "elapsed_seconds": elapsed,
        "status": success,
    }

    with open(os.path.join(temp_path, "build_urls_" + build_name + '.json'), 'w') as build_links:
        json.dump(result, build_links)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    repo_path = os.getenv("REPO_COPY", os.path.abspath("../../"))
    temp_path = os.getenv("TEMP_PATH", os.path.abspath("."))
    caches_path = os.getenv("CACHES_PATH", temp_path)

    build_check_name = sys.argv[1]
    build_name = sys.argv[2]

    build_config = get_build_config(build_check_name, build_name)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    pr_info = PRInfo(get_event())

    logging.info("Repo copy path %s", repo_path)

    gh = Github(get_best_robot_token())
    s3_helper = S3Helper('https://s3.amazonaws.com')

    version = get_version_from_repo(repo_path)
    release_or_pr = None
    if 'release' in pr_info.labels or 'release-lts' in pr_info.labels:
        # for release pull requests we use branch names prefixes, not pr numbers
        release_or_pr = pr_info.head_ref
    elif pr_info.number == 0:
        # for pushes to master - major version
        release_or_pr = ".".join(version.as_tuple()[:2])
    else:
        # PR number for anything else
        release_or_pr = str(pr_info.number)

    s3_path_prefix = "/".join((release_or_pr, pr_info.sha, build_name))

    # If this is rerun, then we try to find already created artifacts and just
    # put them as github actions artifcat (result)
    build_results = get_build_results_if_exists(s3_helper, s3_path_prefix)
    if build_results is not None and len(build_results) > 0:
        logging.info("Some build results found %s", build_results)
        build_urls = []
        log_url = ''
        for url in build_results:
            if 'build_log.log' in url:
                log_url = 'https://s3.amazonaws.com/clickhouse-builds/' +  url.replace('+', '%2B').replace(' ', '%20')
            else:
                build_urls.append('https://s3.amazonaws.com/clickhouse-builds/' + url.replace('+', '%2B').replace(' ', '%20'))
        create_json_artifact(temp_path, build_name, log_url, build_urls, build_config, 0, True)
        sys.exit(0)

    image_name = get_image_name(build_config)
    docker_image = get_image_with_version(os.getenv("IMAGES_PATH"), image_name)
    image_version = docker_image.version

    logging.info("Got version from repo %s", version.get_version_string())

    version_type = 'testing'
    if 'release' in pr_info.labels or 'release-lts' in pr_info.labels:
        version_type = 'stable'

    update_version_local(repo_path, pr_info.sha, version, version_type)

    logging.info("Updated local files with version")

    logging.info("Build short name %s", build_name)

    build_output_path = os.path.join(temp_path, build_name)
    if not os.path.exists(build_output_path):
        os.makedirs(build_output_path)

    ccache_path = os.path.join(caches_path, build_name + '_ccache')

    logging.info("Will try to fetch cache for our build")
    get_ccache_if_not_exists(ccache_path, s3_helper, pr_info.number, temp_path)

    if not os.path.exists(ccache_path):
        logging.info("cache was not fetched, will create empty dir")
        os.makedirs(ccache_path)

    packager_cmd = get_packager_cmd(build_config, os.path.join(repo_path, "docker/packager"), build_output_path, version.get_version_string(), image_version, ccache_path, pr_info)
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


    logging.info("Will upload cache")
    upload_ccache(ccache_path, s3_helper, pr_info.number, temp_path)

    if os.path.exists(log_path):
        log_url = s3_helper.upload_build_file_to_s3(log_path, s3_path_prefix + "/" + os.path.basename(log_path))
        logging.info("Log url %s", log_url)
    else:
        logging.info("Build log doesn't exist")

    build_urls = s3_helper.upload_build_folder_to_s3(build_output_path, s3_path_prefix, keep_dirs_in_s3_path=False, upload_symlinks=False)
    logging.info("Got build URLs %s", build_urls)

    print("::notice ::Build URLs: {}".format('\n'.join(build_urls)))

    print("::notice ::Log URL: {}".format(log_url))

    create_json_artifact(temp_path, build_name, log_url, build_urls, build_config, elapsed, success)
    # Fail build job if not successeded
    if not success:
        sys.exit(1)
