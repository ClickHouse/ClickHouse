import json
from pathlib import Path

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_path = Path(f"{Utils.cwd()}/ci/tmp")

# The release binary is statically linked against the bundled musl, so the glibc
# symbol-version audit and the "does it start on an ancient distribution" checks
# that this job used to run are settled at link time (see the `readelf` assertion
# in the build job). What remains worth checking at runtime is rseq: the bundled
# libc registers a per-thread rseq area with the kernel itself, and `sched_getcpu`
# (hence the per-CPU counters) falls back to a slower path when that fails. The
# warning in `system.warnings` is the observable signal, so exercise both sides:
# registration succeeds regardless of the distribution's own libc, and a kernel
# that refuses the syscall is reported.
RSEQ_WARNING_QUERY = "select count() from system.warnings where message like '%rseq%'"

# Docker seccomp profile that makes the `rseq` syscall fail with ENOSYS, which is
# what a pre-4.18 kernel or a restrictive sandbox looks like to the process.
SECCOMP_NO_RSEQ = {
    "defaultAction": "SCMP_ACT_ALLOW",
    "syscalls": [{"names": ["rseq"], "action": "SCMP_ACT_ERRNO", "errnoRet": 38}],
}


def rseq_check(image: str, expect_rseq: bool, extra_args: str = "") -> str:
    condition = "count()" if expect_rseq else "not(count())"
    return (
        f"docker run --rm --volume={temp_path}/clickhouse:/clickhouse {extra_args} {image} "
        f'/clickhouse local --query "select throwIf({condition}) from ({RSEQ_WARNING_QUERY})"'
    )


def main():
    stopwatch = Utils.Stopwatch()

    for package in temp_path.iterdir():
        if package.suffix == ".deb":
            Shell.check(
                f"dpkg -x {package} {temp_path} && rm {package}",
                verbose=True,
                strict=True,
            )
    Shell.check(
        f"mv {temp_path}/usr/bin/clickhouse {temp_path}/clickhouse",
        verbose=True,
        strict=True,
    )

    seccomp_profile = temp_path / "seccomp_no_rseq.json"
    seccomp_profile.write_text(json.dumps(SECCOMP_NO_RSEQ), encoding="utf-8")

    test_results = []

    if Utils.is_amd():
        # No aarch64 image exists for this release; on amd64 it shows that the
        # registration does not depend on the distribution's glibc (2.15 here).
        test_results.append(
            Result.from_commands_run(
                name="ubuntu12 (rseq available)",
                command=[rseq_check("ubuntu:12.04", expect_rseq=True)],
                with_info=True,
            )
        )

    test_results.append(
        Result.from_commands_run(
            name="ubuntu22 (rseq available)",
            command=[rseq_check("ubuntu:22.04", expect_rseq=True)],
            with_info=True,
        )
    )
    test_results.append(
        Result.from_commands_run(
            name="ubuntu22 (rseq blocked by seccomp)",
            command=[
                rseq_check(
                    "ubuntu:22.04",
                    expect_rseq=False,
                    extra_args=f"--security-opt seccomp={seccomp_profile}",
                )
            ],
            with_info=True,
        )
    )

    Result.create_from(results=test_results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
