#!/usr/bin/env python3
# pylint: disable=line-too-long

import os
import subprocess
import multiprocessing
from tempfile import NamedTemporaryFile
import pytest


CPU_ID = 4


def run_command_in_container(cmd, *args):
    # /clickhouse is mounted by integration tests runner
    alternative_binary = os.getenv("CLICKHOUSE_BINARY", "/clickhouse")
    if alternative_binary:
        args += (
            "--volume",
            f"{alternative_binary}:/usr/bin/clickhouse",
        )

    return subprocess.check_output(
        [
            "docker",
            "run",
            "--rm",
            *args,
            "ubuntu:20.04",
            "sh",
            "-c",
            cmd,
        ]
    )


def run_with_cpu_limit(cmd, *args):
    with NamedTemporaryFile() as online_cpu:
        # NOTE: this is not the number of CPUs, but specific CPU ID
        online_cpu.write(f"{CPU_ID}".encode())
        online_cpu.flush()

        # replace /sys/devices/system/cpu/online to full _SC_NPROCESSORS_ONLN
        # like LXD/LXC from [1] does.
        #
        #   [1]: https://github.com/ClickHouse/ClickHouse/issues/32806
        args += (
            "--volume",
            f"{online_cpu.name}:/sys/devices/system/cpu/online",
        )

        return run_command_in_container(cmd, *args)


def skip_if_jemalloc_disabled():
    output = run_command_in_container(
        """clickhouse local -q "
        SELECT value FROM system.build_options WHERE name = 'USE_JEMALLOC'"
    """
    ).strip()
    if output != b"ON" and output != b"1":
        pytest.skip(f"Compiled without jemalloc (USE_JEMALLOC={output})")


# Ensure that clickhouse works even when number of online CPUs
# (_SC_NPROCESSORS_ONLN) is smaller then available (_SC_NPROCESSORS_CONF).
#
# Refs: https://github.com/jemalloc/jemalloc/pull/2181
def test_jemalloc_percpu_arena():
    skip_if_jemalloc_disabled()

    assert multiprocessing.cpu_count() > CPU_ID

    online_cpus = int(run_with_cpu_limit("getconf _NPROCESSORS_ONLN"))
    assert online_cpus == 1, online_cpus

    all_cpus = int(run_with_cpu_limit("getconf _NPROCESSORS_CONF"))
    assert all_cpus == multiprocessing.cpu_count(), all_cpus

    # implicitly disable percpu arena
    result = run_with_cpu_limit(
        'clickhouse local -q "select 1"',
        # NOTE: explicitly disable, since it is enabled by default in debug build
        # (and even though debug builds are not in CI let's state this).
        "--env",
        "MALLOC_CONF=abort_conf:false",
    )
    assert int(result) == int(1), result

    # should fail because of abort_conf:true
    with pytest.raises(subprocess.CalledProcessError):
        run_with_cpu_limit(
            'clickhouse local -q "select 1"', "--env", "MALLOC_CONF=abort_conf:true"
        )

    # should not fail even with abort_conf:true, due to explicit narenas
    # NOTE: abort:false to make it compatible with debug build
    run_with_cpu_limit(
        'clickhouse local -q "select 1"',
        "--env",
        f"MALLOC_CONF=abort_conf:true,abort:false,narenas:{all_cpus}",
    )


# For manual run.
if __name__ == "__main__":
    test_jemalloc_percpu_arena()
