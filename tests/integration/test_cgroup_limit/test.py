import logging
import math
import os
import subprocess

from helpers.test_tools import wait_condition


def run_command_in_container(cmd, *args):
    # /clickhouse is mounted by integration tests runner
    alternative_binary = os.getenv("CLICKHOUSE_BINARY", "/clickhouse")
    if alternative_binary:
        args += (
            "--volume",
            f"{alternative_binary}:/usr/bin/clickhouse",
        )

    command = [
        "docker",
        "run",
        "--rm",
        *args,
        "ubuntu:22.04",
        "sh",
        "-c",
        cmd,
    ]

    logging.debug("Command: %s", " ".join(command))
    return subprocess.check_output(command)


def run_with_cpu_limit(cmd, num_cpus, *args):
    args += (
        "--cpus",
        f"{num_cpus}",
    )
    return run_command_in_container(cmd, *args)


def test_cgroup_cpu_limit():
    for num_cpus in (1, 2, 4, 2.8):
        def run_with_retry():
            result = run_with_cpu_limit(
                "clickhouse local -q \"select value from system.settings where name='max_threads'\"",
                num_cpus,
            )
            expect_output = (r"\'auto({})\'".format(math.ceil(num_cpus))).encode()
            return (
                result.strip() == expect_output
            ), f"fail for cpu limit={num_cpus}, result={result.strip()}, expect={expect_output}"

        wait_condition(run_with_retry, lambda x:x, max_attempts=3, delay=0.2)


# For manual run
if __name__ == "__main__":
    test_cgroup_cpu_limit()
