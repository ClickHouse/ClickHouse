#!/usr/bin/env python3
"""Measure how long the `clickhouse` binary takes to start up, and compare two builds.

Startup cost is invisible to the performance-comparison harness: that one runs its queries over a
persistent client connection against servers that are already up, so nothing there ever spawns the
binary. This measures the whole `execve`-to-exit cost of short-lived invocations instead
(`clickhouse local --query "SELECT 1"` and friends), which is what a user waits for on every call.

Metrics come from `getrusage(RUSAGE_CHILDREN)`, so no `perf` (absent from the CI images) and no extra
packages are needed:

  cpu     `ru_utime + ru_stime` - CPU actually burnt. Much steadier than wall clock, because it does
          not count time the runner spent scheduling something else.
  minflt  `ru_minflt` - minor page faults. Nearly deterministic for a fixed binary (it is dominated
          by faulting in the executable and zeroing anonymous memory), so it catches a regression
          even when a noisy runner makes the timings useless.
  wall    Reported for context only. Never used to decide pass/fail.

Both `cpu` and `minflt` gate the result, with very different thresholds, because they fail in
opposite directions - see `MINFLT_RATIO_THRESHOLD` and `CPU_RATIO_THRESHOLD` below.

Two builds are measured in alternating rounds (`left`, `right`, `left`, ...) rather than one after the
other, so that drift in runner speed hits both sides equally. Per-round ratios are then combined with
a median, which ignores the occasional round that lost its CPU to a neighbour.
"""

import argparse
import os
import resource
import statistics
import subprocess
import sys
from dataclasses import dataclass


@dataclass
class Sample:
    """Per-invocation cost, averaged over one round."""

    cpu: float
    minflt: float
    wall: float


# Scenarios worth watching. Each one is a full process lifetime - start-up, the work, and tear-down.
# These need no server, so they can run anywhere.
HERMETIC_SCENARIOS = {
    # `local` and `client` share almost all of their start-up (dynamic relocations, ~3000 static
    # initialisers) but diverge in `main`, so both are worth measuring: `local` builds a Context and
    # databases, `client` sets up connection handling instead. Neither runs a query here, which
    # isolates process start-up and shut-down from anything query related.
    "local_version": ["local", "--version"],
    "client_version": ["client", "--version"],
    # The cheapest possible real query. Builds a Context, resolves `system.one`, runs, tears down.
    "local_select_1": ["local", "--query", "SELECT 1"],
    # Same, but reaching for a table that forces the whole `system` database to be built.
    "local_system_tables": ["local", "--query", "SELECT count() FROM system.tables"],
}

# Needs a running server, whose address is substituted for `{port}`. The server is held fixed while
# the two client builds are swapped, so server-side cost is a constant that cancels in the ratio.
# This is the only scenario that exercises connecting and handshaking, which is where a client pays
# for `ClientInfo` (including resolving its own hostname) and for name resolution.
SERVER_SCENARIOS = {
    "client_select_1": ["client", "--port", "{port}", "--query", "SELECT 1"],
}

SCENARIOS = {**HERMETIC_SCENARIOS, **SERVER_SCENARIOS}


def scenario_args(name: str, server_port: int = 0) -> list:
    """Command-line arguments for a scenario, with the server port filled in."""
    return [arg.format(port=server_port) for arg in SCENARIOS[name]]


def _run_once(binary: str, args: list) -> None:
    subprocess.run(
        [binary] + args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=True,
    )


def measure_round(binary: str, args: list, iterations: int) -> Sample:
    """Spawn the binary `iterations` times and return the average cost of one invocation.

    `RUSAGE_CHILDREN` accumulates over every child this process has reaped, so nothing else may run
    concurrently while this is measuring.
    """
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall_before = os.times().elapsed
    for _ in range(iterations):
        _run_once(binary, args)
    wall_after = os.times().elapsed
    after = resource.getrusage(resource.RUSAGE_CHILDREN)

    cpu = (after.ru_utime - before.ru_utime) + (after.ru_stime - before.ru_stime)
    return Sample(
        cpu=cpu / iterations,
        minflt=(after.ru_minflt - before.ru_minflt) / iterations,
        wall=(wall_after - wall_before) / iterations,
    )


def measure(
    left: str,
    right: str,
    args: list,
    rounds: int,
    iterations: int,
    warmup: int,
) -> tuple:
    """Alternate between the two builds for `rounds` rounds. Returns (left samples, right samples)."""
    # Warm the page cache for both binaries first. Without this the first round pays for faulting in
    # a ~1GB executable and would dominate the result.
    for binary in (left, right):
        for _ in range(warmup):
            _run_once(binary, args)

    left_samples, right_samples = [], []
    for round_index in range(rounds):
        # Swap which build goes first on alternate rounds, so a systematic first-in-round penalty
        # cannot be attributed to one side. Measure positionally rather than keyed by path: the two
        # paths are equal when comparing a build against itself to check the noise floor.
        if round_index % 2 == 0:
            left_sample = measure_round(left, args, iterations)
            right_sample = measure_round(right, args, iterations)
        else:
            right_sample = measure_round(right, args, iterations)
            left_sample = measure_round(left, args, iterations)
        left_samples.append(left_sample)
        right_samples.append(right_sample)
    return left_samples, right_samples


# Two gates, because the two metrics fail in opposite directions.
#
# Minor faults are the trustworthy one: they count work done rather than time taken, so a busy
# neighbour cannot move them. Measured against itself the ratio stays inside 0.3%, which is why the
# threshold can be tight - and it is the gate that actually catches the usual shape of a start-up
# regression (more static initialisation, more objects built, more relocations), all of which touch
# more memory.
MINFLT_RATIO_THRESHOLD = 1.05
# CPU time is the noisy one. It is steadier than wall clock, since it excludes time spent waiting to
# be scheduled, but it is not immune on a shared runner: cache and memory-bandwidth contention make
# the same work burn more cycles, and frequency scaling shifts the whole scale. So it gets a loose
# threshold and exists to catch what faults cannot - a regression that burns CPU without touching new
# memory.
CPU_RATIO_THRESHOLD = 1.15


def _median(samples: list, field: str) -> float:
    return statistics.median(getattr(s, field) for s in samples)


def regression_reasons(
    row: dict, cpu_threshold: float, minflt_threshold: float
) -> list:
    """Human-readable reasons this scenario should fail, empty if it is fine."""
    reasons = []
    if row["minflt_ratio"] >= minflt_threshold:
        reasons.append(
            f"minor faults {row['left_minflt']:.0f} -> {row['right_minflt']:.0f} "
            f"({row['minflt_ratio']:.3f}x, threshold {minflt_threshold:.3f}x)"
        )
    if row["cpu_ratio"] >= cpu_threshold:
        reasons.append(
            f"cpu {row['left_cpu_ms']:.2f}ms -> {row['right_cpu_ms']:.2f}ms "
            f"({row['cpu_ratio']:.3f}x, threshold {cpu_threshold:.2f}x)"
        )
    return reasons


def compare(left_samples: list, right_samples: list, field: str) -> float:
    """Median of the per-round ratios. >1 means `right` costs more than `left`."""
    ratios = [
        getattr(r, field) / getattr(l, field)
        for l, r in zip(left_samples, right_samples)
        if getattr(l, field) > 0
    ]
    return statistics.median(ratios) if ratios else float("nan")


def run_scenario(
    name: str,
    left: str,
    right: str,
    rounds: int,
    iterations: int,
    warmup: int,
    server_port: int = 0,
) -> dict:
    args = scenario_args(name, server_port)
    left_samples, right_samples = measure(left, right, args, rounds, iterations, warmup)
    return {
        "scenario": name,
        "left_cpu_ms": _median(left_samples, "cpu") * 1000,
        "right_cpu_ms": _median(right_samples, "cpu") * 1000,
        "cpu_ratio": compare(left_samples, right_samples, "cpu"),
        "left_minflt": _median(left_samples, "minflt"),
        "right_minflt": _median(right_samples, "minflt"),
        "minflt_ratio": compare(left_samples, right_samples, "minflt"),
        "left_wall_ms": _median(left_samples, "wall") * 1000,
        "right_wall_ms": _median(right_samples, "wall") * 1000,
    }


def format_table(rows: list) -> str:
    header = (
        f"{'scenario':<22} {'cpu left':>9} {'cpu right':>9} {'ratio':>7}   "
        f"{'flt left':>9} {'flt right':>9} {'ratio':>7}"
    )
    lines = [header, "-" * len(header)]
    for r in rows:
        lines.append(
            f"{r['scenario']:<22} {r['left_cpu_ms']:>8.2f}m {r['right_cpu_ms']:>8.2f}m "
            f"{r['cpu_ratio']:>7.3f}   {r['left_minflt']:>9.0f} {r['right_minflt']:>9.0f} "
            f"{r['minflt_ratio']:>7.3f}"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--left", required=True, help="reference binary (e.g. master)")
    parser.add_argument(
        "--right", required=True, help="binary under test (e.g. the PR)"
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=sorted(SCENARIOS),
        help="scenario to measure; repeatable, defaults to all",
    )
    parser.add_argument(
        "--rounds", type=int, default=11, help="alternating rounds per scenario"
    )
    parser.add_argument(
        "--iterations", type=int, default=30, help="invocations per build per round"
    )
    parser.add_argument(
        "--warmup", type=int, default=5, help="untimed invocations per build"
    )
    parser.add_argument(
        "--cpu-threshold",
        type=float,
        default=CPU_RATIO_THRESHOLD,
        help="fail if the median CPU-time ratio is at least this",
    )
    parser.add_argument(
        "--minflt-threshold",
        type=float,
        default=MINFLT_RATIO_THRESHOLD,
        help="fail if the median minor-fault ratio is at least this",
    )
    parser.add_argument(
        "--server-port",
        type=int,
        default=0,
        help=f"port of a running server; required for {sorted(SERVER_SCENARIOS)}",
    )
    parser.add_argument("--tsv", help="also write the results as TSV to this path")
    args = parser.parse_args()

    scenarios = args.scenario or sorted(
        SCENARIOS if args.server_port else HERMETIC_SCENARIOS
    )
    needs_server = [s for s in scenarios if s in SERVER_SCENARIOS]
    if needs_server and not args.server_port:
        parser.error(f"--server-port is required for {needs_server}")

    rows = [
        run_scenario(
            name,
            args.left,
            args.right,
            args.rounds,
            args.iterations,
            args.warmup,
            args.server_port,
        )
        for name in scenarios
    ]

    print(format_table(rows))

    if args.tsv:
        with open(args.tsv, "w", encoding="utf-8") as f:
            fields = list(rows[0])
            f.write("\t".join(fields) + "\n")
            for r in rows:
                f.write("\t".join(str(r[k]) for k in fields) + "\n")

    verdicts = [
        (r["scenario"], reason)
        for r in rows
        for reason in regression_reasons(r, args.cpu_threshold, args.minflt_threshold)
    ]
    if verdicts:
        print("")
        for scenario, reason in verdicts:
            print(f"FAIL: {scenario}: {reason}")
        return 1
    print(
        f"\nOK: no scenario reached {args.cpu_threshold:.2f}x CPU or "
        f"{args.minflt_threshold:.3f}x minor faults"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
