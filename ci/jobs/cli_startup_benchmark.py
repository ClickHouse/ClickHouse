#!/usr/bin/env python3
"""CI job: compare `clickhouse` start-up cost against the master build.

The performance-comparison job cannot see start-up cost at all: it runs its queries over a persistent
client connection against servers that are already running, so nothing in it ever spawns the binary,
and `clickhouse local` is never exercised. This job measures whole short-lived invocations instead.

It follows the same shape as `performance_tests.py`: the build under test comes in as the job artifact
("right"), the reference comes from the most recent master build on S3 ("left"), and the two are
measured in alternating rounds so runner drift cancels out. See
`ci/jobs/scripts/cli_startup/measure.py` for the measurement itself and why CPU time and minor page
faults are used rather than wall clock.
"""

import argparse
import sys
from pathlib import Path

from ci.jobs.scripts.cli_startup import measure
from ci.jobs.scripts.clickhouse_service import ClickHouseService
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

temp_dir = f"{Utils.cwd()}/ci/tmp"
reference_dir = f"{temp_dir}/cli_startup_reference"
results_tsv = f"{temp_dir}/cli_startup_metrics.tsv"

# The server that the `client_*` scenarios talk to. It is the build under test in both cases - held
# fixed while the two client binaries are swapped - so its cost is a constant that cancels in the
# ratio.
SERVER_PORT = 9000


def find_reference_build(info, build_type):
    """Most recent master build of `build_type` that actually has an uploaded binary."""
    commits = info.get_kv_data("master_track_commits_sha") or []
    for sha in commits:
        link = f"https://clickhouse-builds.s3.us-east-1.amazonaws.com/REFs/master/{sha}/{build_type}/clickhouse"
        if Shell.check(f"curl -sfI {link} > /dev/null"):
            return link, sha
    return None, ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ch-path", default=temp_dir, help="directory holding the built binary"
    )
    parser.add_argument("--rounds", type=int, default=11)
    parser.add_argument("--iterations", type=int, default=30)
    args = parser.parse_args()

    stopwatch = Utils.Stopwatch()
    results = []

    right = f"{args.ch_path}/clickhouse"
    assert Path(right).is_file(), f"No binary under test at {right}"
    # CI release artifacts arrive without the executable bit.
    Path(right).chmod(0o755)

    info = Info()
    build_type = "build_arm_release" if Utils.is_arm() else "build_amd_release"
    link, reference_sha = find_reference_build(info, build_type)

    if not link:
        # Without a reference there is nothing to compare against. Report it rather than inventing a
        # verdict from absolute numbers, which are meaningless on a shared runner.
        Result.create_from(
            results=[],
            stopwatch=stopwatch,
            status=Result.Status.SKIPPED,
            info=f"No master {build_type} binary available to compare against",
        ).complete_job()
        return 0

    left = f"{reference_dir}/clickhouse"
    results.append(
        Result.from_commands_run(
            name="Prepare binaries",
            command=[
                f"mkdir -p {reference_dir}",
                f"wget -nv -O {left} {link}",
                f"chmod +x {left}",
                # Both builds are self-extracting: the first run replaces the ~1GB wrapper in place
                # with the multi-GB real ELF. Do that here, deliberately and once per binary, so
                # that no measured round ever pays for decompression. Also needs the disk for it.
                f"{left} local --version",
                f"{right} local --version",
            ],
        )
    )

    if not results[-1].is_ok():
        Result.create_from(results=results, stopwatch=stopwatch).complete_job()
        return 1

    def run(scenario, server_port=0):
        sw = Utils.Stopwatch()
        row = measure.run_scenario(
            scenario, left, right, args.rounds, args.iterations, 5, server_port
        )
        reasons = measure.regression_reasons(
            row, measure.CPU_RATIO_THRESHOLD, measure.MINFLT_RATIO_THRESHOLD
        )
        # One sub-result per scenario, so the CI page shows which invocation shape moved and why.
        results.append(
            Result(
                name=scenario,
                status=Result.Status.FAIL if reasons else Result.Status.OK,
                start_time=sw.start_time,
                duration=sw.duration,
                info=(
                    "; ".join(reasons)
                    if reasons
                    else (
                        f"cpu {row['left_cpu_ms']:.2f}ms -> {row['right_cpu_ms']:.2f}ms "
                        f"({row['cpu_ratio']:.3f}x), "
                        f"minor faults {row['left_minflt']:.0f} -> {row['right_minflt']:.0f} "
                        f"({row['minflt_ratio']:.3f}x)"
                    )
                ),
            )
        )
        return row

    rows = [run(name) for name in sorted(measure.HERMETIC_SCENARIOS)]

    # The client scenarios need something to connect to. Start the build under test once and measure
    # both clients against it.
    with ClickHouseService(
        results=results,
        config_hooks=[ClickHouseService.install_base],
    ):
        rows += [run(name, SERVER_PORT) for name in sorted(measure.SERVER_SCENARIOS)]

    print(measure.format_table(rows))

    with open(results_tsv, "w", encoding="utf-8") as f:
        fields = list(rows[0])
        f.write("\t".join(fields) + "\n")
        for row in rows:
            f.write("\t".join(str(row[k]) for k in fields) + "\n")

    Result.create_from(
        results=results,
        stopwatch=stopwatch,
        files=[results_tsv],
        info=f"reference: {reference_sha or 'unknown'} ({build_type})",
    ).complete_job()
    return 0


if __name__ == "__main__":
    sys.exit(main())
