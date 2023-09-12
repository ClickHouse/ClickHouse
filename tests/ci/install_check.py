#!/usr/bin/env python3

import argparse

import atexit
import logging
import sys
import subprocess
from pathlib import Path
from shutil import copy2
from typing import Dict

from github import Github

from build_download_helper import download_builds_filter
from clickhouse_helper import (
    ClickHouseHelper,
    mark_flaky_tests,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    RerunHelper,
    format_description,
    get_commit,
    post_commit_status,
    update_mergeable_check,
)
from compress_files import compress_fast
from docker_pull_helper import get_image_with_version, DockerImage
from env_helper import CI, TEMP_PATH as TEMP, REPORTS_PATH
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResults, TestResult, FAILURE, FAIL, OK, SUCCESS
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results


RPM_IMAGE = "clickhouse/install-rpm-test"
DEB_IMAGE = "clickhouse/install-deb-test"
TEMP_PATH = Path(TEMP)
LOGS_PATH = TEMP_PATH / "tests_logs"


def prepare_test_scripts():
    server_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
systemctl start clickhouse-server
clickhouse-client -q 'SELECT version()'"""
    keeper_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
systemctl start clickhouse-keeper
for i in {1..20}; do
    echo wait for clickhouse-keeper to being up
    > /dev/tcp/127.0.0.1/9181 2>/dev/null && break || sleep 1
done
for i in {1..5}; do
    echo wait for clickhouse-keeper to answer on mntr request
    {
        exec 13<>/dev/tcp/127.0.0.1/9181
        echo mntr >&13
        cat <&13 | grep zk_version
    } && break || sleep 1
    exec 13>&-
done
exec 13>&-"""
    binary_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
/packages/clickhouse.copy install
clickhouse-server start --daemon
for i in {1..5}; do
    clickhouse-client -q 'SELECT version()' && break || sleep 1
done
clickhouse-keeper start --daemon
for i in {1..20}; do
    echo wait for clickhouse-keeper to being up
    > /dev/tcp/127.0.0.1/9181 2>/dev/null && break || sleep 1
done
for i in {1..5}; do
    echo wait for clickhouse-keeper to answer on mntr request
    {
        exec 13<>/dev/tcp/127.0.0.1/9181
        echo mntr >&13
        cat <&13 | grep zk_version
    } && break || sleep 1
    exec 13>&-
done
exec 13>&-"""
    preserve_logs = r"""#!/bin/bash
journalctl -u clickhouse-server > /tests_logs/clickhouse-server.service || :
journalctl -u clickhouse-keeper > /tests_logs/clickhouse-keeper.service || :
cp /var/log/clickhouse-server/clickhouse-server.* /tests_logs/ || :
cp /var/log/clickhouse-keeper/clickhouse-keeper.* /tests_logs/ || :
chmod a+rw -R /tests_logs
exit 1
"""
    (TEMP_PATH / "server_test.sh").write_text(server_test, encoding="utf-8")
    (TEMP_PATH / "keeper_test.sh").write_text(keeper_test, encoding="utf-8")
    (TEMP_PATH / "binary_test.sh").write_text(binary_test, encoding="utf-8")
    (TEMP_PATH / "preserve_logs.sh").write_text(preserve_logs, encoding="utf-8")


def test_install_deb(image: DockerImage) -> TestResults:
    tests = {
        "Install server deb": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common}*deb
bash -ex /packages/server_test.sh""",
        "Install keeper deb": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-keeper*deb
bash -ex /packages/keeper_test.sh""",
        "Install clickhouse binary in deb": r"bash -ex /packages/binary_test.sh",
    }
    return test_install(image, tests)


def test_install_rpm(image: DockerImage) -> TestResults:
    # FIXME: I couldn't find why Type=notify is broken in centos:8
    # systemd just ignores the watchdog completely
    tests = {
        "Install server rpm": r"""#!/bin/bash -ex
yum localinstall --disablerepo=* -y /packages/clickhouse-{server,client,common}*rpm
echo CLICKHOUSE_WATCHDOG_ENABLE=0 > /etc/default/clickhouse-server
bash -ex /packages/server_test.sh""",
        "Install keeper rpm": r"""#!/bin/bash -ex
yum localinstall --disablerepo=* -y /packages/clickhouse-keeper*rpm
bash -ex /packages/keeper_test.sh""",
        "Install clickhouse binary in rpm": r"bash -ex /packages/binary_test.sh",
    }
    return test_install(image, tests)


def test_install_tgz(image: DockerImage) -> TestResults:
    # FIXME: I couldn't find why Type=notify is broken in centos:8
    # systemd just ignores the watchdog completely
    tests = {
        f"Install server tgz in {image.name}": r"""#!/bin/bash -ex
[ -f /etc/debian_version ] && CONFIGURE=configure || CONFIGURE=
for pkg in /packages/clickhouse-{common,client,server}*tgz; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh" $CONFIGURE
done
[ -f /etc/yum.conf ] && echo CLICKHOUSE_WATCHDOG_ENABLE=0 > /etc/default/clickhouse-server
bash -ex /packages/server_test.sh""",
        f"Install keeper tgz in {image.name}": r"""#!/bin/bash -ex
[ -f /etc/debian_version ] && CONFIGURE=configure || CONFIGURE=
for pkg in /packages/clickhouse-keeper*tgz; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh" $CONFIGURE
done
bash -ex /packages/keeper_test.sh""",
    }
    return test_install(image, tests)


def test_install(image: DockerImage, tests: Dict[str, str]) -> TestResults:
    test_results = []  # type: TestResults
    for name, command in tests.items():
        stopwatch = Stopwatch()
        container_name = name.lower().replace(" ", "_").replace("/", "_")
        log_file = TEMP_PATH / f"{container_name}.log"
        logs = [log_file]
        run_command = (
            f"docker run --rm --privileged --detach --cap-add=SYS_PTRACE "
            f"--volume={LOGS_PATH}:/tests_logs --volume={TEMP_PATH}:/packages {image}"
        )

        for retry in range(1, 4):
            for file in LOGS_PATH.glob("*"):
                file.unlink()

            logging.info("Running docker container: `%s`", run_command)
            container_id = subprocess.check_output(
                run_command, shell=True, encoding="utf-8"
            ).strip()
            (TEMP_PATH / "install.sh").write_text(command)
            install_command = (
                f"docker exec {container_id} bash -ex /packages/install.sh"
            )
            with TeePopen(install_command, log_file) as process:
                retcode = process.wait()
                if retcode == 0:
                    status = OK
                    break

                status = FAIL
            copy2(log_file, LOGS_PATH)
            archive_path = TEMP_PATH / f"{container_name}-{retry}.tar.gz"
            compress_fast(LOGS_PATH, archive_path)
            logs.append(archive_path)

        subprocess.check_call(f"docker kill -s 9 {container_id}", shell=True)
        test_results.append(TestResult(name, status, stopwatch.duration_seconds, logs))

    return test_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="The script to check if the packages are able to install",
    )

    parser.add_argument(
        "check_name",
        help="check name, used to download the packages",
    )
    parser.add_argument("--download", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-download",
        dest="download",
        action="store_false",
        default=argparse.SUPPRESS,
        help="if set, the packages won't be downloaded, useful for debug",
    )
    parser.add_argument("--deb", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-deb",
        dest="deb",
        action="store_false",
        default=argparse.SUPPRESS,
        help="if set, the deb packages won't be checked",
    )
    parser.add_argument("--rpm", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-rpm",
        dest="rpm",
        action="store_false",
        default=argparse.SUPPRESS,
        help="if set, the rpm packages won't be checked",
    )
    parser.add_argument("--tgz", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-tgz",
        dest="tgz",
        action="store_false",
        default=argparse.SUPPRESS,
        help="if set, the tgz packages won't be checked",
    )

    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    args = parse_args()

    TEMP_PATH.mkdir(parents=True, exist_ok=True)
    LOGS_PATH.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()

    if CI:
        gh = Github(get_best_robot_token(), per_page=100)
        commit = get_commit(gh, pr_info.sha)
        atexit.register(update_mergeable_check, gh, pr_info, args.check_name)

        rerun_helper = RerunHelper(commit, args.check_name)
        if rerun_helper.is_already_finished_by_status():
            logging.info(
                "Check is already finished according to github status, exiting"
            )
            sys.exit(0)

    docker_images = {
        name: get_image_with_version(REPORTS_PATH, name)
        for name in (RPM_IMAGE, DEB_IMAGE)
    }
    prepare_test_scripts()

    if args.download:

        def filter_artifacts(path: str) -> bool:
            is_match = False
            if args.deb or args.rpm:
                is_match = is_match or path.endswith("/clickhouse")
            if args.deb:
                is_match = is_match or path.endswith(".deb")
            if args.rpm:
                is_match = is_match or path.endswith(".rpm")
            if args.tgz:
                is_match = is_match or path.endswith(".tgz")
            return is_match

        download_builds_filter(
            args.check_name, REPORTS_PATH, TEMP_PATH, filter_artifacts
        )

    test_results = []  # type: TestResults
    ch_binary = Path(TEMP_PATH) / "clickhouse"
    if ch_binary.exists():
        # make a copy of clickhouse to avoid redownload of exctracted binary
        ch_binary.chmod(0o755)
        ch_copy = ch_binary.parent / "clickhouse.copy"
        copy2(ch_binary, ch_binary.parent / "clickhouse.copy")
        subprocess.check_output(f"{ch_copy.absolute()} local -q 'SELECT 1'", shell=True)

    if args.deb:
        test_results.extend(test_install_deb(docker_images[DEB_IMAGE]))
    if args.rpm:
        test_results.extend(test_install_rpm(docker_images[RPM_IMAGE]))
    if args.tgz:
        test_results.extend(test_install_tgz(docker_images[DEB_IMAGE]))
        test_results.extend(test_install_tgz(docker_images[RPM_IMAGE]))

    state = SUCCESS
    test_status = OK
    description = "Packages installed successfully"
    if FAIL in (result.status for result in test_results):
        state = FAILURE
        test_status = FAIL
        description = "Failed to install packages: " + ", ".join(
            result.name for result in test_results
        )

    s3_helper = S3Helper()

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [],
        args.check_name,
    )
    print(f"::notice ::Report url: {report_url}")
    if not CI:
        return

    ch_helper = ClickHouseHelper()
    mark_flaky_tests(ch_helper, args.check_name, test_results)

    description = format_description(description)

    post_commit_status(commit, state, report_url, description, args.check_name, pr_info)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        test_status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        args.check_name,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
