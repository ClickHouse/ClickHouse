import argparse
from pathlib import Path
from typing import Dict, List

from ci.jobs.scripts.docker_image import DockerImage
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

RPM_IMAGE = "clickhouse/install-rpm-test"
DEB_IMAGE = "clickhouse/install-deb-test"
REPO_PATH = Utils.cwd()
TEMP_PATH = Path(f"{REPO_PATH}/ci/tmp/")


def prepare_test_scripts():
    server_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
test_env='TEST_THE_DEFAULT_PARAMETER=15'
echo "$test_env" >> /etc/default/clickhouse
# Note, clickhouse-server service notify systemd only when it is ready to
# accept connections, so we do not need to wait until it will open the port for
# listening manually here.
systemctl restart clickhouse-server
clickhouse-client -q 'SELECT version()'
grep "$test_env" /proc/$(cat /var/run/clickhouse-server/clickhouse-server.pid)/environ"""
    initd_via_systemd_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
test_env='TEST_THE_DEFAULT_PARAMETER=15'
echo "$test_env" >> /etc/default/clickhouse
# Note, this should use systemctl
/etc/init.d/clickhouse-server start
clickhouse-client -q 'SELECT version()'
grep "$test_env" /proc/$(cat /var/run/clickhouse-server/clickhouse-server.pid)/environ"""
    initd_test = r"""#!/bin/bash
set -e
trap "bash -ex /packages/preserve_logs.sh" ERR
test_env='TEST_THE_DEFAULT_PARAMETER=15'
echo "$test_env" >> /etc/default/clickhouse
# Do not use systemd, and hence we need to wait until the server will be ready below
SYSTEMCTL_SKIP_REDIRECT=1 /etc/init.d/clickhouse-server start
for i in {1..5}; do
    clickhouse-client -q 'SELECT version()' && break || sleep 1
done
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
/packages/clickhouse install
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
journalctl -u clickhouse-server > /packages/clickhouse-server.service.log || :
journalctl -u clickhouse-keeper > /packages/clickhouse-keeper.service.log || :
cp /var/log/clickhouse-server/clickhouse-server.* /packages/ || :
cp /var/log/clickhouse-keeper/clickhouse-keeper.* /packages/ || :
chmod a+rw -R /packages
exit 1
"""
    (TEMP_PATH / "server_test.sh").write_text(server_test, encoding="utf-8")
    (TEMP_PATH / "initd_via_systemd_test.sh").write_text(
        initd_via_systemd_test, encoding="utf-8"
    )
    (TEMP_PATH / "initd_test.sh").write_text(initd_test, encoding="utf-8")
    (TEMP_PATH / "keeper_test.sh").write_text(keeper_test, encoding="utf-8")
    (TEMP_PATH / "binary_test.sh").write_text(binary_test, encoding="utf-8")
    (TEMP_PATH / "preserve_logs.sh").write_text(preserve_logs, encoding="utf-8")


def test_install_deb(image: DockerImage) -> List[Result]:
    tests = {
        "Install server deb": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common}*deb
bash -ex /packages/server_test.sh""",
        "Run server init.d (proxy to systemd)": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common}*deb
bash -ex /packages/initd_via_systemd_test.sh""",
        "Run server init.d": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common}*deb
bash -ex /packages/initd_test.sh""",
        "Install keeper deb": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-keeper*deb
bash -ex /packages/keeper_test.sh""",
        "Install clickhouse binary in deb": r"bash -ex /packages/binary_test.sh",
    }
    return test_install(image, tests)


def test_install_rpm(image: DockerImage) -> List[Result]:
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


def test_install_tgz(image: DockerImage) -> List[Result]:
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


def test_install(image: DockerImage, tests: Dict[str, str]) -> List[Result]:
    test_results = []  # type: List[Result]
    for name, command in tests.items():
        run_command = (
            f"docker run --rm --privileged --detach --cap-add=SYS_PTRACE "
            f"--volume={TEMP_PATH}:/packages {image}"
        )
        print(f"Running docker container: [{run_command}]")
        container_id = Shell.get_output(run_command, verbose=True, strict=True)
        test_script_path = TEMP_PATH / "install.sh"
        test_script_path.write_text(command)
        # Shell.check(f"chmod +x {test_script_path}")
        install_command = f"docker exec {container_id} bash -ex /packages/install.sh"
        test_results.append(
            Result.from_commands_run(
                name=name,
                command=install_command,
            )
        )
        Shell.check(f"docker kill -s 9 {container_id}", verbose=True)
    return test_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="The script to check if the packages are able to install",
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
    stopwatch = Utils.Stopwatch()

    args = parse_args()

    deb_image = DockerImage.get_docker_image(DEB_IMAGE).pull_image()
    rpm_image = DockerImage.get_docker_image(RPM_IMAGE).pull_image()

    Shell.check(f"chmod +x {Utils.cwd()}/ci/tmp/clickhouse", verbose=True, strict=True)

    prepare_test_scripts()

    test_results = []  # type: List[Result]

    if args.deb:
        print("Test debian")
        test_results.extend(test_install_deb(deb_image))
    if args.rpm:
        print("Test rpm")
        test_results.extend(test_install_rpm(rpm_image))
    if args.tgz:
        print("Test tgz")
        test_results.extend(test_install_tgz(deb_image))
        test_results.extend(test_install_tgz(rpm_image))

    Result.create_from(
        results=test_results,
        stopwatch=stopwatch,
        files=[
            f for f in TEMP_PATH.iterdir() if f.is_file() and f.name.endswith(".log")
        ],
    ).complete_job()


if __name__ == "__main__":
    main()
