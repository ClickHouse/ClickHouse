"""Helpers for soak tests of server resources under repeated data lake operations.

A soak runs the same operation many times and compares an early window of iterations with a late one. A leak shows up as growth between the windows, while a bounded cache or pool stops growing once it is warm. Everything compared here is a count or a size, never a duration.

Two kinds of signals are collected:

- Server-wide samples taken between iterations (`ResourceSampler`): `MemoryTracking`, the resident memory, the open file descriptors and the threads of the server process, and any extra `system.metrics` values (cache sizes, pool sizes, retained background tasks).
- Per-query signals from `system.query_log` (`QueryLog`): `memory_usage` and `ProfileEvents` such as object storage request counts. A per-iteration request count that grows with the iteration number means that some state is re-read or re-listed in full by every operation instead of incrementally.

The number of iterations comes from an environment variable (see `soak_iterations`), so a pull request runs a short soak and a nightly run a long one with the same code.
"""

import logging
import os
import statistics
import uuid
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence

MiB = 1024 * 1024

# AddressSanitizer keeps freed memory in a quarantine to detect use after free, up to `quarantine_size_mb` (256 MiB by default on 64-bit targets, ClickHouse does not change it in `base/base/sanitizer_options.h`). The quarantine fills with the churn of allocations, not with leaks, and its memory stays resident, so on an ASan build the resident memory may grow by this much without any leak.
ASAN_QUARANTINE_BYTES = 256 * MiB


def soak_iterations(env_var: str, default: int, minimum: int = 10) -> int:
    """Number of soak iterations: `env_var` if set, `default` otherwise.

    The compared windows need some iterations each, so a value below `minimum` is a configuration error rather than something to raise silently."""
    raw = os.environ.get(env_var, "").strip()
    value = int(raw) if raw else default
    assert value >= minimum, f"{env_var}={value} is below the minimum of {minimum} iterations"
    logging.info("Soak iterations: %d (%s=%r)", value, env_var, raw)
    return value


def windows(values: Sequence, warmup_fraction: float = 0.2, window_fraction: float = 0.25):
    """Split `values` into an early and a late window.

    The first `warmup_fraction` is skipped: it fills caches, connection pools and thread pools, which is growth that happens once and is not a leak. The early window follows the warmup, the late window is the tail."""
    n = len(values)
    warmup = int(n * warmup_fraction)
    size = max(2, int(n * window_fraction))
    assert warmup + 2 * size <= n, f"{n} samples are too few for two windows of {size} after a warmup of {warmup}"
    return list(values[warmup : warmup + size]), list(values[n - size :])


def assert_not_growing(
    name: str,
    values: Sequence[float],
    abs_tolerance: float,
    rel_tolerance: float = 0.0,
    statistic=statistics.median,
    unit: str = "",
):
    """Assert that the late window is not larger than the early one beyond a tolerance.

    The allowed growth is `abs_tolerance + rel_tolerance * early`. The median of each window is compared by default, so a single outlier (a retried request, a log flush landing in one iteration) does not decide the result, while growth that persists for a whole window does."""
    early, late = windows(values)
    early_value = statistic(early)
    late_value = statistic(late)
    allowed = abs_tolerance + rel_tolerance * early_value
    growth = late_value - early_value
    message = (
        f"{name}: early {early_value:.1f}{unit}, late {late_value:.1f}{unit}, growth {growth:.1f}{unit}, "
        f"allowed {allowed:.1f}{unit} over {len(values)} samples; early window {list(early)}, late window {list(late)}"
    )
    logging.info(message)
    assert growth <= allowed, message
    return early_value, late_value


# Prints, for the server process `PID`: its open file descriptors except those of per-thread procfs files, the per-thread procfs descriptors, those of them that belong to threads that already exited, and then `RssAnon`, `RssFile` and `Threads` from its status (resident memory in KiB). A thread that collects its own statistics keeps descriptors of files under `/proc/PID/task/<tid>/` open, so their number follows the number of threads that have run queries and is checked through the threads, unless their thread is gone.
SAMPLE_SCRIPT = r"""
set -e
test -d /proc/PID
targets=$(for f in /proc/PID/fd/*; do readlink "$f" || true; done)
thread_files=$(echo "$targets" | grep '^/proc/PID/task/' || true)
echo "$targets" | grep -vc '^/proc/PID/task/' || true
echo "$thread_files" | grep -c . || true
echo "$thread_files" | sed -nE 's#^/proc/PID/task/([0-9]+)/.*#\1#p' | while read -r tid; do test -d "/proc/PID/task/$tid" || echo "$tid"; done | grep -c . || true
awk '/^(RssAnon|RssFile|Threads):/ {print $1, $2}' /proc/PID/status
"""


@dataclass
class Sample:
    iteration: int
    memory_tracking: int
    rss: int
    rss_file: int
    fds: int
    exited_thread_fds: int
    threads: int
    metrics: Dict[str, int] = field(default_factory=dict)


class ResourceSampler:
    """Samples resources of the server process of `node` between soak iterations."""

    def __init__(self, node, metrics: Iterable[str] = ()):
        self.node = node
        self.metrics = sorted(set(metrics) | {"MemoryTracking"})
        self.pid = node.get_process_pid("clickhouse")
        assert self.pid is not None, "cannot find the server process"
        self.asan = node.is_built_with_sanitizer("address")
        self.samples: List[Sample] = []

    def sample(self, iteration: int) -> Sample:
        names = ", ".join(f"'{m}'" for m in self.metrics)
        rows = self.node.query(f"SELECT metric, value FROM system.metrics WHERE metric IN ({names})")
        values = {}
        for line in rows.strip().splitlines():
            metric, value = line.split("\t")
            values[metric] = int(value)
        missing = set(self.metrics) - set(values)
        assert not missing, f"metrics are absent from system.metrics: {sorted(missing)}"

        # The process must be the one found at the start: a restart in the middle resets every value and would hide a leak.
        out = self.node.exec_in_container(["bash", "-c", SAMPLE_SCRIPT.replace("PID", str(self.pid))])
        lines = out.strip().splitlines()
        fds, _thread_fds, exited_thread_fds = (int(x) for x in lines[:3])
        status = {k: int(v) for k, v in (line.split() for line in lines[3:] if line)}
        sample = Sample(
            iteration=iteration,
            memory_tracking=values.pop("MemoryTracking"),
            rss=status["RssAnon:"] * 1024,
            rss_file=status["RssFile:"] * 1024,
            fds=fds,
            exited_thread_fds=exited_thread_fds,
            threads=status["Threads:"],
            metrics=values,
        )
        self.samples.append(sample)
        return sample

    def series(self, name: str) -> List[int]:
        if name in ("memory_tracking", "rss", "rss_file", "fds", "exited_thread_fds", "threads"):
            return [getattr(s, name) for s in self.samples]
        return [s.metrics[name] for s in self.samples]

    def open_file_kinds(self) -> str:
        """What the open file descriptors of the server point to, grouped (`socket`, `anon_inode:[eventfd]`, directories of regular files), most frequent first."""
        return self.node.exec_in_container(
            [
                "bash",
                "-c",
                f"for f in /proc/{self.pid}/fd/*; do readlink \"$f\"; done | sed -E 's/:\\[[0-9]+\\]$//; s#/[^/]*$#/#' | sort | uniq -c | sort -rn | head -20",
            ]
        )

    def socket_peers(self) -> str:
        """The TCP sockets of the server: listening ones by port, the others by remote port and state (hex, as in `/proc/net/tcp`, `01` is established), most frequent first. An outgoing connection has the port of the remote service as its remote port, for example 9001 for MinIO."""
        script = r"""
inodes=" $(for f in /proc/PID/fd/*; do readlink "$f"; done | sed -nE 's/^socket:\[([0-9]+)\]$/\1/p' | tr '\n' ' ')"
cat /proc/PID/net/tcp /proc/PID/net/tcp6 | while read -r _ local remote state _ _ _ _ _ inode _; do
    case "$inodes" in *" $inode "*)
        if [ "$state" = 0A ]; then echo "listening on $((16#${local##*:}))"; else echo "remote port $((16#${remote##*:})) state $state"; fi;;
    esac
done | sort | uniq -c | sort -rn | head -20
"""
        return self.node.exec_in_container(["bash", "-c", script.replace("PID", str(self.pid))])

    def thread_names(self) -> str:
        """Names of the threads of the server, grouped, most frequent first."""
        return self.node.exec_in_container(
            ["bash", "-c", f"cat /proc/{self.pid}/task/*/comm | sort | uniq -c | sort -rn | head -20"]
        )

    def log_summary(self, label: str):
        for name in ["memory_tracking", "rss", "rss_file", "fds", "exited_thread_fds", "threads"] + [m for m in self.metrics if m != "MemoryTracking"]:
            logging.info("%s %s: %s", label, name, self.series(name))
        logging.info("%s open files by kind:\n%s", label, self.open_file_kinds())
        logging.info("%s TCP sockets:\n%s", label, self.socket_peers())
        logging.info("%s threads by name:\n%s", label, self.thread_names())

    def assert_bounded(
        self,
        memory_abs_tolerance: int = 64 * MiB,
        memory_rel_tolerance: float = 0.1,
        fds_tolerance: int = 16,
        threads_tolerance: int = 16,
        cache_metrics: Sequence[str] = (),
    ):
        """Assert that the server-wide resources do not grow from the early to the late window.

        Memory gets a tolerance in bytes plus a fraction of the early value: allocator caching, log buffers and the query that just ran move it by some megabytes between samples. File descriptors and threads come from bounded pools (connection pools, thread pools), so after the warmup their maximum moves by a few at most.

        `cache_metrics` name `system.metrics` sizes in bytes of caches that legitimately fill up during the soak, for example `IcebergMetadataFilesCacheBytes` while every `INSERT` adds a manifest. Their size is subtracted from the memory before the comparison, as the caches have limits of their own."""
        caches = [sum(s.metrics[m] for m in cache_metrics) for s in self.samples]
        assert_not_growing(
            "MemoryTracking without caches",
            [m - c for m, c in zip(self.series("memory_tracking"), caches)],
            abs_tolerance=memory_abs_tolerance,
            rel_tolerance=memory_rel_tolerance,
            unit=" B",
        )
        # Resident memory also covers allocations that `MemoryTracking` does not see, such as those of the Rust Delta kernel. Only anonymous memory is compared: file-backed pages are those of the binary and of the files it reads, which the server faults in on first use (for example the debug info when it symbolizes a stack trace) and the kernel can evict, so they are not held by the server.
        assert_not_growing(
            "anonymous resident memory without caches",
            [m - c for m, c in zip(self.series("rss"), caches)],
            abs_tolerance=memory_abs_tolerance + (ASAN_QUARANTINE_BYTES if self.asan else 0),
            rel_tolerance=memory_rel_tolerance,
            unit=" B",
        )
        assert_not_growing("open file descriptors", self.series("fds"), abs_tolerance=fds_tolerance, statistic=max)
        assert_not_growing("descriptors of exited threads", self.series("exited_thread_fds"), abs_tolerance=fds_tolerance, statistic=max)
        assert_not_growing("threads", self.series("threads"), abs_tolerance=threads_tolerance, statistic=max)


class QueryLog:
    """Runs soak queries under recognizable query ids and reads their `system.query_log` rows.

    Queries go over HTTP, which is much cheaper than a `docker exec` of `clickhouse-client` per query when there are hundreds of iterations."""

    def __init__(self, node, label: str):
        self.node = node
        self.prefix = f"soak_{label}_{uuid.uuid4().hex[:8]}"

    def query(self, operation: str, iteration: int, sql: str, settings: Optional[Dict] = None) -> str:
        params = dict(settings or {})
        params["query_id"] = f"{self.prefix}:{operation}:{iteration}:{uuid.uuid4().hex[:6]}"
        return self.node.http_query(sql, method="POST", params=params)

    def per_iteration(self, operation: str, events: Sequence[str]) -> Dict[str, List[int]]:
        """Per-iteration `memory_usage` and `ProfileEvents` of `operation`, ordered by iteration.

        Each value is the sum over the queries of the iteration, so an operation that runs several statements per iteration still gives one number per iteration."""
        self.node.query("SYSTEM FLUSH LOGS query_log")
        columns = ", ".join(f"sum(ProfileEvents['{e}'])" for e in events)
        rows = self.node.query(
            f"""
            SELECT toUInt64(splitByChar(':', query_id)[3]) AS iteration, sum(memory_usage), {columns}
            FROM system.query_log
            WHERE event_date >= yesterday() AND type = 'QueryFinish'
              AND startsWith(query_id, '{self.prefix}:{operation}:')
            GROUP BY iteration ORDER BY iteration
            """
        )
        result: Dict[str, List[int]] = {"iteration": [], "memory_usage": [], **{e: [] for e in events}}
        for line in rows.strip().splitlines():
            parts = [int(x) for x in line.split("\t")]
            result["iteration"].append(parts[0])
            result["memory_usage"].append(parts[1])
            for e, v in zip(events, parts[2:]):
                result[e].append(v)
        return result

    def assert_constant_per_iteration(
        self,
        operation: str,
        events: Sequence[str],
        expected_iterations: int,
        events_tolerance: int = 2,
        memory_abs_tolerance: int = 8 * MiB,
        memory_rel_tolerance: float = 0.5,
        require_nonzero: Sequence[str] = (),
    ) -> Dict[str, List[int]]:
        """Assert that each event count and the memory of `operation` stay flat over the soak.

        `require_nonzero` names events that the operation must produce at all, so that a soak that stopped reaching object storage, and therefore measures nothing, does not pass."""
        data = self.per_iteration(operation, events)
        assert len(data["iteration"]) == expected_iterations, (
            f"{operation}: {len(data['iteration'])} finished iterations in query_log, expected {expected_iterations}"
        )
        for e in require_nonzero:
            assert sum(data[e]) > 0, f"{operation}: no {e} during the soak, the check would measure nothing"
        for e in events:
            assert_not_growing(f"{operation} {e} per iteration", data[e], abs_tolerance=events_tolerance)
        assert_not_growing(
            f"{operation} memory_usage per iteration",
            data["memory_usage"],
            abs_tolerance=memory_abs_tolerance,
            rel_tolerance=memory_rel_tolerance,
            unit=" B",
        )
        return data
