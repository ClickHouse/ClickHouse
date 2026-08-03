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
# Do not use systemd, and hence we need to wait until the server will be ready below.
# The init.d wrapper prints "Server started" once the pid file exists, but the TCP
# listener can take longer to open on a slow CI host; poll for up to 30s. See #86278.
SYSTEMCTL_SKIP_REDIRECT=1 /etc/init.d/clickhouse-server start
for i in {1..30}; do
    clickhouse-client --receive_timeout=5 -q 'SELECT version()' && break || sleep 1
done
clickhouse-client --receive_timeout=5 -q 'SELECT version()'
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
    clickhouse-client --receive_timeout=5 -q 'SELECT version()' && break || sleep 1
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
    # `clickhouse-common-static-dbg` unpacks to 3.4 GiB and takes around 40 seconds to
    # install - longer than everything else in a test together. `Install server deb`
    # covers it, and the two tests below differ from it only in the way the server is
    # started, so they install the same packages without the debug symbols.
    tests = {
        "Install server deb": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common}*deb -y
bash -ex /packages/server_test.sh""",
        "Run server init.d (proxy to systemd)": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common-static}_*deb -y
bash -ex /packages/initd_via_systemd_test.sh""",
        "Run server init.d": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-{server,client,common-static}_*deb -y
bash -ex /packages/initd_test.sh""",
        "Install keeper deb": r"""#!/bin/bash -ex
apt-get install /packages/clickhouse-keeper*deb -y
bash -ex /packages/keeper_test.sh""",
    }
    return test_install(image, tests)


def test_install_rpm(image: DockerImage) -> List[Result]:
    # FIXME: I couldn't find why Type=notify is broken in centos:8
    # systemd just ignores the watchdog completely
    tests = {
        "Install server rpm": r"""#!/bin/bash -ex
yum localinstall --disablerepo=* --allowerasing -y /packages/clickhouse-{server,client,common}*rpm
echo CLICKHOUSE_WATCHDOG_ENABLE=0 > /etc/default/clickhouse-server
bash -ex /packages/server_test.sh""",
        "Install keeper rpm": r"""#!/bin/bash -ex
yum localinstall --disablerepo=* --allowerasing -y /packages/clickhouse-keeper*rpm
bash -ex /packages/keeper_test.sh""",
    }
    return test_install(image, tests)


def test_install_binary(image: DockerImage, name: str) -> List[Result]:
    # By far the most disk hungry test of the job. The self-extracting archive unpacks in
    # place, so `/packages/clickhouse` grows from 1 GiB to 4 GiB, and `clickhouse install`
    # needs as much again: it hard links the binary into `/usr/bin` when it can, but
    # `/packages` is a directory mounted from the host, so the link cannot be made and the
    # binary is copied instead. On top of that, the installer refuses to start unless the
    # whole size of the binary is available, which is why this test runs last - see `main`.
    return test_install(image, {name: r"bash -ex /packages/binary_test.sh"})


def test_install_tgz(image: DockerImage, debug_symbols: bool) -> List[Result]:
    # FIXME: I couldn't find why Type=notify is broken in centos:8
    # systemd just ignores the watchdog completely

    # `doinst.sh` copies the unpacked tree into the system instead of moving it, so a
    # package occupies twice its size - 6.8 GiB for `clickhouse-common-static-dbg` -
    # until the tree is removed. These are by far the most disk hungry tests of the
    # job, and they used to fail with `No space left on device`. The debug symbols also
    # take more than a minute to install, and the same tarballs are installed in both
    # images, so unpack them only in the first one - see `main`.
    server_packages = (
        "clickhouse-{common,client,server}*tgz"
        if debug_symbols
        else "clickhouse-{common-static,client,server}-[0-9]*tgz"
    )
    keeper_packages = (
        "clickhouse-keeper*tgz" if debug_symbols else "clickhouse-keeper-[0-9]*tgz"
    )
    tests = {
        f"Install server tgz in {image}": r"""#!/bin/bash -ex
[ -f /etc/debian_version ] && CONFIGURE=configure || CONFIGURE=
for pkg in /packages/@PACKAGES@; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh" $CONFIGURE
    rm -rf "/${package:?}"
done
[ -f /etc/yum.conf ] && echo CLICKHOUSE_WATCHDOG_ENABLE=0 > /etc/default/clickhouse-server
bash -ex /packages/server_test.sh""".replace("@PACKAGES@", server_packages),
        f"Install keeper tgz in {image}": r"""#!/bin/bash -ex
[ -f /etc/debian_version ] && CONFIGURE=configure || CONFIGURE=
for pkg in /packages/@PACKAGES@; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh" $CONFIGURE
    rm -rf "/${package:?}"
done
bash -ex /packages/keeper_test.sh""".replace("@PACKAGES@", keeper_packages),
        f"Install tgz over a symlinked config in {image}": r"""#!/bin/bash -ex
# An installation may keep its config elsewhere and link to it from the installed path.
# The installer has to write through such a symlink instead of replacing it.
mkdir -p /etc/clickhouse-client /shared
: > /shared/config.xml
ln -s /shared/config.xml /etc/clickhouse-client/config.xml
for pkg in /packages/clickhouse-client*tgz; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh"
done
[ -L /etc/clickhouse-client/config.xml ]
[ "$(readlink /etc/clickhouse-client/config.xml)" = "/shared/config.xml" ]
[ -s /shared/config.xml ]""",
        f"Install tgz over a hard-linked config in {image}": r"""#!/bin/bash -ex
# An installed file may have other hard links to it, and replacing the destination by a
# rename would leave them pointing at the pre-upgrade contents. The installer has to write
# through such a destination so that every link sees the new contents.
mkdir -p /etc/clickhouse-client /backup
echo "<clickhouse></clickhouse>" > /etc/clickhouse-client/config.xml
ln /etc/clickhouse-client/config.xml /backup/config.xml
inode=$(stat -c %i /etc/clickhouse-client/config.xml)
for pkg in /packages/clickhouse-client*tgz; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh"
done
[ "$(stat -c %i /etc/clickhouse-client/config.xml)" = "$inode" ]
[ "$(stat -c %h /etc/clickhouse-client/config.xml)" = "2" ]
# `cmp`/`diff` are not installed in the minimal images, compare the contents in the shell.
[ "$(cat /etc/clickhouse-client/config.xml)" = "$(cat /backup/config.xml)" ]
[ "$(cat /etc/clickhouse-client/config.xml)" != "<clickhouse></clickhouse>" ]""",
        f"Install tgz over a dangling symlink in {image}": r"""#!/bin/bash -ex
# A symlink whose target does not exist yet has to survive the installation as well: `-e`
# follows the link, so the installer must test for the link itself before testing existence.
mkdir -p /etc/clickhouse-client /shared
ln -s /shared/config.xml /etc/clickhouse-client/config.xml
for pkg in /packages/clickhouse-client*tgz; do
    package=${pkg%-*}
    package=${package##*/}
    tar xf "$pkg"
    "/$package/install/doinst.sh"
done
[ -L /etc/clickhouse-client/config.xml ]
[ "$(readlink /etc/clickhouse-client/config.xml)" = "/shared/config.xml" ]
[ -s /shared/config.xml ]""",
    }
    return test_install(image, tests)


def test_install(image: DockerImage, tests: Dict[str, str]) -> List[Result]:
    test_results = []  # type: List[Result]
    for name, command in tests.items():
        # Note, `--rm` is deliberately not used: it reclaims the writable layer of the
        # container asynchronously, while every test writes several gigabytes of
        # installed files there and the next test starts right away. The layer is
        # removed synchronously by `docker rm` below instead.
        run_command = (
            f"docker run --privileged --detach --cap-add=SYS_PTRACE "
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
        # Strict: if the writable layer cannot be reclaimed, the job is about to run out of
        # disk space, and the failure has to surface here rather than in the next test.
        Shell.check(
            f"docker rm --force --volumes {container_id}", verbose=True, strict=True
        )
        # The job runs out of disk space from time to time, and the failure surfaces as
        # an unrelated error in whatever test happens to be running, so keep track of it.
        Shell.check("df -h /", verbose=True)
    return test_results


def free_packages(pattern: str) -> None:
    """Delete the packages that have already been tested.

    All the package flavours are downloaded into the same directory, which is mounted
    into every container, and together they take more than 6 GiB - as much as a single
    container needs to install them. Nothing reads a package once its own tests are
    done, so drop it to leave room for the tests that follow.
    """
    Shell.check(f"rm -f {TEMP_PATH}/{pattern}", verbose=True)
    Shell.check("df -h /", verbose=True)


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
        free_packages("*.deb")
    if args.rpm:
        print("Test rpm")
        test_results.extend(test_install_rpm(rpm_image))
        free_packages("*.rpm")
    if args.tgz:
        print("Test tgz")
        # The tgz packages are the same on both distributions, so the debug symbols are
        # installed once - the second run only checks that `doinst.sh` works outside of
        # Debian, for which the debug symbols add nothing but two minutes.
        test_results.extend(test_install_tgz(deb_image, debug_symbols=True))
        test_results.extend(test_install_tgz(rpm_image, debug_symbols=False))
        free_packages("*.tgz*")

    # The binary tests need nothing but `/packages/clickhouse`, and they need around 7 GiB
    # of disk for it, more than twice as much as any other test here. The runners hand the
    # job as little as 4 GiB of free space, so run these tests once every package has been
    # deleted, which is worth 7 GiB on its own. Keeping the binary packed until the very
    # end leaves 3 GiB more for the tgz tests as well.
    print("Test the binary")
    if args.deb:
        test_results.extend(
            test_install_binary(deb_image, "Install clickhouse binary in deb")
        )
    if args.rpm:
        test_results.extend(
            test_install_binary(rpm_image, "Install clickhouse binary in rpm")
        )

    Result.create_from(
        results=test_results,
        stopwatch=stopwatch,
        files=[
            f for f in TEMP_PATH.iterdir() if f.is_file() and f.name.endswith(".log")
        ],
    ).complete_job()


if __name__ == "__main__":
    main()
