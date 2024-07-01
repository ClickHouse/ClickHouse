#!/usr/bin/env python3

import argparse
import logging
import subprocess
import sys
from pathlib import Path
from shutil import copy2
from typing import Dict

from build_download_helper import download_builds_filter
from compress_files import compress_fast
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPORT_PATH
from env_helper import TEMP_PATH as TEMP
from report import FAIL, FAILURE, OK, SUCCESS, JobReport, TestResult, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen

RPM_IMAGE = "clickhouse/install-rpm-test"
DEB_IMAGE = "clickhouse/install-deb-test"
TEMP_PATH = Path(TEMP)
LOGS_PATH = TEMP_PATH / "tests_logs"


def prepare_test_scripts():
    server_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
test_env='TEST_THE_DEFAULT_PARAMETER=15'
echo "$test_env" >> /etc/default/clickhouse
systemctl restart clickhouse-server
clickhouse-client -q 'SELECT version()'
grep "$test_env" /proc/$(cat /var/run/clickhouse-server/clickhouse-server.pid)/environ"""
    initd_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
test_env='TEST_THE_DEFAULT_PARAMETER=15'
echo "$test_env" >> /etc/default/clickhouse
/etc/init.d/clickhouse-server start
clickhouse-client -q 'SELECT version()'
grep "$test_env" /proc/$(cat /var/run/clickhouse-server/clickhouse-server.pid)/environ"""
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
    (TEMP_PATH / "initd_test.sh").write_text(initd_test, encoding="utf-8")
    (TEMP_PATH / "keeper_test.sh").write_text(keeper_test, encoding="utf-8")
    (TEMP_PATH / "binary_test.sh").write_text(binary_test, encoding="utf-8")
    (TEMP_PATH / "preserve_logs.sh").write_text(preserve_logs, encoding="utf-8")


def test_install_deb(image: DockerImage) -> TestResults:
    tests = {
        "Install server deb": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common}*deb
bash -ex /packages/server_test.sh""",
        "Run server init.d": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common}*deb
bash -ex /packages/initd_test.sh""",
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
        f"Install server tgz in {image}": r"""#!/bin/bash -ex
[ -f /etc/debian_version ] && CONFIGURE=configure || CONFIGURE=
for pkg in /packages/clickhouse-{common,client,server}*tgz; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh" $CONFIGURE
done
[ -f /etc/yum.conf ] && echo CLICKHOUSE_WATCHDOG_ENABLE=0 > /etc/default/clickhouse-server
bash -ex /packages/server_test.sh""",
        f"Install keeper tgz in {image}": r"""#!/bin/bash -ex
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
                    subprocess.check_call(
                        f"docker kill -s 9 {container_id}", shell=True
                    )
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

    deb_image = pull_image(get_docker_image(DEB_IMAGE))
    rpm_image = pull_image(get_docker_image(RPM_IMAGE))

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
            # We don't need debug packages, so let's filter them out
            is_match = is_match and "-dbg" not in path
            return is_match

        download_builds_filter(
            args.check_name, REPORT_PATH, TEMP_PATH, filter_artifacts
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
        test_results.extend(test_install_deb(deb_image))
    if args.rpm:
        test_results.extend(test_install_rpm(rpm_image))
    if args.tgz:
        test_results.extend(test_install_tgz(deb_image))
        test_results.extend(test_install_tgz(rpm_image))

    state = SUCCESS
    description = "Packages installed successfully"
    if FAIL in (result.status for result in test_results):
        state = FAILURE
        description = "Failed to install packages: " + ", ".join(
            result.name for result in test_results
        )

    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[],
    ).dump()

    if state == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
