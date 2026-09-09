#!/usr/bin/env python3
"""In-memory `partitioned_hash` vs `parallel_hash` phase-split sweep.

Drives a `clickhouse-server` on a remote benchmark host over SSH (full
sampling-profiler support requires the server; `clickhouse local` never
starts the trace collector). Keys are drawn from a persistent
`keys_store` database of MergeTree tables (created once by `prepare-keys`,
reused across every cell); `Memory`-engine build/probe tables are recreated
per cell and filled from that store. Every command is stdlib-only Python.

Commands: plan, prepare-keys, run-cell, sweep, selftest, report.
See `--help` on each subcommand for its flags.

ONE plan exists: 347 distinct cells, deduplicated by cell_id and sharded
across N instances via `plan --shards N [--shard-summary]`
/ `sweep --shard k --shards N`: one `sweep` process per instance, each
with its own `--ssh-host` and its own `--results-path`;
`report --results a.jsonl,b.jsonl,...` merges them.

Multi-architecture: fleets of DIFFERENT CPU architectures (e.g. one
aarch64 fleet and one x86_64 fleet, each with its own shards) may sweep
CONCURRENTLY from one orchestration host. Each sweep auto-detects the
remote architecture (`uname -m`), stamps it into every results row, and
keeps per-cell log exports under `cells/<arch>/<cell_id>/` so the two
fleets never collide on disk. Architectures are never mixed within one
benchmark: `report` renders one architecture at a time (`--arch`, or
auto when the results contain only one), and `--coverage` checks each
architecture found in the results independently.
"""

from __future__ import annotations

import argparse
import dataclasses
import decimal
import hashlib
import json
import os
import pathlib
import shlex
import statistics
import subprocess
import sys
import time
from collections.abc import Sequence

DEFAULT_LOCAL_ROOT = "/mnt/data/inmem_sweep"
DEFAULT_REMOTE_CLICKHOUSE = "/home/ubuntu/ch/programs/clickhouse"
DEFAULT_REMOTE_CONFIG = "/home/ubuntu/ch/programs/server/config.xml"
DEFAULT_REMOTE_SERVER_DIR = "/home/ubuntu/ch/programs/server"
DEFAULT_REMOTE_DATA_PATH = "/home/ubuntu/bench/data"
DEFAULT_REMOTE_LOG_DIR = "/home/ubuntu/bench/logs"

ALGORITHMS = ("partitioned_hash", "parallel_hash")
UINT64_MAX = (1 << 64) - 1
# Untimed warmups per (cell, algorithm) before the timed runs. The
# expression JIT compiles an expression on the execution AFTER its seen
# count reaches `min_count_to_compile_expression` (default 3) -- i.e. on
# execution #4, measured twice: with 1 warmup, the compilation cost
# landed on timed run index 2 (execution 4); with 3 warmups it landed on
# timed run index 0 (still execution 4; the first attempt at "3 warmups"
# only looked clean because earlier selftest queries had pre-warmed the
# server-lifetime compiled-expression cache -- real cells restart the
# server, so the cache is cold). 4 warmups put execution #4 inside the
# untimed window; the per-run `CompileExpressionsMicroseconds` counter is
# still recorded so any residual contamination is visible, not silent.
WARMUP_RUNS = 4

# Real ProfileEvents::Event counters. Verified to exist in `system.events` on
# this binary (branch `ahj`) before anything else runs; see `selftest
# --check-events`. `MemoryTrackerPeakUsage` is handled separately below: it is
# a client-protocol pseudo-event (see `MemoryTracker::PEAK_USAGE_EVENT_NAME`),
# not a registered counter, and is never in `system.events` or in
# `system.query_log.ProfileEvents` by construction (verified empirically).
# Peak memory per cell is instead read from `query_log.memory_usage`, which
# is the query's peak `MemoryTracker` value (verified: source + a live query
# with a known allocation showed a nonzero, plausible `memory_usage` while
# `ProfileEvents['MemoryTrackerPeakUsage']` was absent from the map).
MEMORY_TRACKER_PEAK_PSEUDO_EVENT = "MemoryTrackerPeakUsage"

PARALLEL_HASH_EVENTS = (
    "ConcurrentHashJoinBuildMicroseconds",
    "ConcurrentHashJoinBuildDispatchMicroseconds",
    "ConcurrentHashJoinBuildInsertMicroseconds",
    "ConcurrentHashJoinBuildMergeMicroseconds",
    "ConcurrentHashJoinProbeMicroseconds",
    "ConcurrentHashJoinProbeDispatchMicroseconds",
    "ConcurrentHashJoinProbeLookupMicroseconds",
)
PARTITIONED_HASH_EVENTS = (
    "PartitionedHashJoinBuildMicroseconds",
    "PartitionedHashJoinBuildFillMicroseconds",
    "PartitionedHashJoinBuildHistogramMicroseconds",
    "PartitionedHashJoinBuildScatterMicroseconds",
    "PartitionedHashJoinBuildLeafMicroseconds",
    "PartitionedHashJoinProbeMicroseconds",
    "PartitionedHashJoinProbeLookupMicroseconds",
    "PartitionedHashJoinPartitions",
    "PartitionedHashJoinLeafRows",
    "PartitionedHashJoinHashTableGrowths",
    "PartitionedHashJoinDistinctEstimateReused",
)
SHARED_HASH_JOIN_EVENTS = (
    "HashJoinResultBuildOutputMicroseconds",
    "HashJoinResultFilterLeftMicroseconds",
)
# Every real counter the phase split maps onto.
# `MemoryTrackerPeakUsage` is intentionally excluded (see above); it is
# checked and reported through its own, separate, explicitly-labelled path.
ALL_MAPPING_EVENTS = PARALLEL_HASH_EVENTS + PARTITIONED_HASH_EVENTS + SHARED_HASH_JOIN_EVENTS

# Positive execution-path assertions (a cell reporting zero build time on its
# own algorithm's total build event is a red flag, not something to average
# into a median).
PATH_ASSERTION_EVENT = {
    "parallel_hash": "ConcurrentHashJoinBuildMicroseconds",
    "partitioned_hash": "PartitionedHashJoinBuildMicroseconds",
}

# Phase-split formulas: name -> (algorithm -> tuple of event names summed).
PHASE_SPLIT = {
    "build_scatter": {
        "parallel_hash": ("ConcurrentHashJoinBuildDispatchMicroseconds",),
        "partitioned_hash": (
            "PartitionedHashJoinBuildFillMicroseconds",
            "PartitionedHashJoinBuildHistogramMicroseconds",
            "PartitionedHashJoinBuildScatterMicroseconds",
        ),
    },
    "build_insert": {
        "parallel_hash": (
            "ConcurrentHashJoinBuildInsertMicroseconds",
            "ConcurrentHashJoinBuildMergeMicroseconds",
        ),
        "partitioned_hash": ("PartitionedHashJoinBuildLeafMicroseconds",),
    },
    "build_total": {
        "parallel_hash": ("ConcurrentHashJoinBuildMicroseconds",),
        "partitioned_hash": ("PartitionedHashJoinBuildMicroseconds",),
    },
    "probe_lookup": {
        "parallel_hash": (
            "ConcurrentHashJoinProbeDispatchMicroseconds",
            "ConcurrentHashJoinProbeLookupMicroseconds",
        ),
        "partitioned_hash": ("PartitionedHashJoinProbeLookupMicroseconds",),
    },
    "probe_right_gather": {
        "parallel_hash": ("HashJoinResultBuildOutputMicroseconds",),
        "partitioned_hash": ("HashJoinResultBuildOutputMicroseconds",),
    },
    "probe_left_gather": {
        "parallel_hash": ("HashJoinResultFilterLeftMicroseconds",),
        "partitioned_hash": ("HashJoinResultFilterLeftMicroseconds",),
    },
    "probe_total": {
        "parallel_hash": ("ConcurrentHashJoinProbeMicroseconds",),
        "partitioned_hash": ("PartitionedHashJoinProbeMicroseconds",),
    },
}
EXTRA_PARTITIONED_COUNTERS = (
    "PartitionedHashJoinPartitions",
    "PartitionedHashJoinLeafRows",
    "PartitionedHashJoinHashTableGrowths",
    "PartitionedHashJoinDistinctEstimateReused",
)

# Key-config catalogue. N_max: 512M for K0/K7 (the configs the plan takes
# to D=512M), 128M for the rest (raised from 32M so
# every key family gets a D ladder; recorded per-table as a comment on the
# `keys_store` table too). Tables whose row count does not match 2*N_max
# are transparently re-prepared by `prepare-keys`.
K0_K7_N_MAX = 512_000_000
DEFAULT_N_MAX = 128_000_000

KEY_SEED_HIT = 0x9E3779B97F4A7C15
KEY_SEED_MISS = 0xC2B2AE3D27D4EB4F
BUILD_PAYLOAD_SEED = 0xFF51AFD7ED558CCD
PROBE_PAYLOAD_SEED = 0xC4CEB9FE1A85EC53


@dataclasses.dataclass(frozen=True)
class KeyConfig:
    key_id: str
    kind: str  # "numeric" | "string" | "nullable_numeric" | "nullable_string"
    ncols: int  # numeric column count (numeric/nullable_numeric)
    strlen: int  # string byte length (string/nullable_string)
    n_max: int
    method_hint: str  # informational: expected internal join method name

    @property
    def key_columns(self) -> tuple[str, ...]:
        if self.kind in ("numeric", "nullable_numeric"):
            return tuple(f"k{i}" for i in range(self.ncols))
        return ("k",)

    def column_type(self, nullable_wrap: bool) -> str:
        base = "String" if self.kind in ("string", "nullable_string") else "UInt64"
        if nullable_wrap:
            return f"Nullable({base})"
        return base

    @property
    def is_nullable(self) -> bool:
        return self.kind in ("nullable_numeric", "nullable_string")


KEY_CONFIGS: dict[str, KeyConfig] = {
    "K0": KeyConfig("K0", "numeric", 1, 0, K0_K7_N_MAX, "key64"),
    "K1": KeyConfig("K1", "numeric", 2, 0, DEFAULT_N_MAX, "keys128"),
    "K2": KeyConfig("K2", "numeric", 4, 0, DEFAULT_N_MAX, "keys256"),
    "K3": KeyConfig("K3", "numeric", 8, 0, DEFAULT_N_MAX, "hashed"),
    "K4": KeyConfig("K4", "string", 0, 8, DEFAULT_N_MAX, "key_string"),
    "K5": KeyConfig("K5", "string", 0, 16, DEFAULT_N_MAX, "key_string"),
    "K6": KeyConfig("K6", "string", 0, 32, DEFAULT_N_MAX, "key_string"),
    "K7": KeyConfig("K7", "string", 0, 64, K0_K7_N_MAX, "key_string"),
    "K8": KeyConfig("K8", "nullable_numeric", 1, 0, DEFAULT_N_MAX, "key64 (nullable)"),
    "K9": KeyConfig("K9", "nullable_string", 0, 16, DEFAULT_N_MAX, "key_string (nullable)"),
}

ANCHOR = dict(D=32_000_000, key="K0", m_b=1, m_p=1, h="1.0", bp=8, pp=8)


# --------------------------------------------------------------------------
# SSH / remote execution
# --------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class RemoteArgs:
    ssh_key: str
    ssh_host: str
    clickhouse: str
    config: str
    server_dir: str
    data_path: str
    log_dir: str
    tcp_port: int = 9000


def remote_args_from_ns(args: argparse.Namespace) -> RemoteArgs:
    return RemoteArgs(
        ssh_key=args.ssh_key,
        ssh_host=args.ssh_host,
        clickhouse=args.remote_clickhouse,
        config=args.remote_config,
        server_dir=args.remote_server_dir,
        data_path=args.remote_data_path,
        log_dir=args.remote_log_dir,
        tcp_port=args.remote_tcp_port,
    )


def add_remote_args(parser: argparse.ArgumentParser) -> None:
    # Deliberately required (no defaults): a default would silently point at
    # whichever instance existed when the default was written -- an instance
    # that is terminated by the time anyone reads this.
    parser.add_argument("--ssh-key", required=True, help="path to the ephemeral private key for this run's instance(s)")
    parser.add_argument("--ssh-host", required=True, help="user@private-ip of the benchmark instance, e.g. ubuntu@172.31.x.y")
    parser.add_argument("--remote-clickhouse", default=DEFAULT_REMOTE_CLICKHOUSE)
    parser.add_argument("--remote-config", default=DEFAULT_REMOTE_CONFIG)
    parser.add_argument("--remote-server-dir", default=DEFAULT_REMOTE_SERVER_DIR)
    parser.add_argument("--remote-data-path", default=DEFAULT_REMOTE_DATA_PATH)
    parser.add_argument("--remote-log-dir", default=DEFAULT_REMOTE_LOG_DIR)
    parser.add_argument(
        "--remote-tcp-port",
        type=int,
        default=9000,
        help="TCP port the benchmark server listens on; the client always connects with an explicit --port so a stray server on the default port can never be measured by accident",
    )


def ssh_base(remote: RemoteArgs) -> list[str]:
    return [
        "ssh",
        "-i",
        remote.ssh_key,
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "BatchMode=yes",
        remote.ssh_host,
    ]


def run_ssh(
    remote: RemoteArgs,
    remote_cmd: str,
    *,
    input_bytes: bytes | None = None,
    timeout: float | None = None,
) -> tuple[int, bytes, str]:
    argv = ssh_base(remote) + [remote_cmd]
    try:
        proc = subprocess.run(
            argv,
            input=input_bytes if input_bytes is not None else b"",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as ex:
        stdout = ex.stdout if isinstance(ex.stdout, bytes) else b""
        return 124, stdout, f"ssh timed out after {timeout}s"
    return proc.returncode, proc.stdout, proc.stderr.decode("utf-8", "replace").strip()


def run_remote_sql(
    remote: RemoteArgs,
    sql: str,
    *,
    settings: str = "",
    timeout: float | None = None,
    query_id: str | None = None,
) -> tuple[int, bytes, str]:
    """Run `sql` (may be multiple `;`-separated statements) via the remote client."""
    client_cmd = f"{shlex.quote(remote.clickhouse)} client --port {remote.tcp_port} --multiquery"
    if query_id:
        client_cmd += f" --query_id={shlex.quote(query_id)}"
    if settings:
        client_cmd += f" {settings}"
    return run_ssh(remote, client_cmd, input_bytes=sql.encode("utf-8"), timeout=timeout)


def run_remote_sql_json(
    remote: RemoteArgs, sql: str, *, timeout: float | None = None
) -> list[dict[str, object]]:
    rc, stdout, stderr = run_remote_sql(remote, sql, timeout=timeout)
    if rc != 0:
        raise RuntimeError(f"remote query failed (rc={rc}): {stderr or 'no diagnostic'}\nSQL: {sql}")
    rows: list[dict[str, object]] = []
    for line in stdout.decode("utf-8", "strict").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def require_ok(rc: int, stderr: str, purpose: str) -> None:
    if rc != 0:
        raise RuntimeError(f"{purpose} failed (rc={rc}): {stderr or 'no diagnostic'}")


# --------------------------------------------------------------------------
# Server lifecycle
# --------------------------------------------------------------------------


def server_is_up(remote: RemoteArgs) -> bool:
    rc, _, _ = run_remote_sql(remote, "SELECT 1 FORMAT Null", timeout=10)
    return rc == 0


def start_server(remote: RemoteArgs, *, timeout: float = 60.0) -> None:
    if server_is_up(remote):
        return
    # Any previous instance must be fully gone (including the watchdog
    # process, which otherwise respawns a killed server child -- see
    # measured finding) before starting a new one, or the
    # new process fails to bind :9000 and the whole SSH call hangs.
    ensure_server_stopped(remote, timeout=timeout)
    script = (
        f"mkdir -p {shlex.quote(remote.log_dir)} {shlex.quote(remote.data_path)} && "
        f"cd {shlex.quote(remote.server_dir)} && "
        f"nohup setsid {shlex.quote(remote.clickhouse)} server -C "
        f"{shlex.quote(remote.config)} </dev/null "
        f">{shlex.quote(remote.log_dir)}/stdout.log 2>&1 &"
    )
    # Deliberately fire-and-forget: even with `nohup setsid ... &`, OpenSSH
    # keeps a session's channel open until every process that inherited a
    # copy of its stdio pipes closes them, and a plain `subprocess.run(...,
    # timeout=...)` here blocks (and eventually times out) waiting for that,
    # even though the *remote shell* itself returns immediately after
    # backgrounding the job (measured: the outer `bash -c` process is gone
    # in well under a second, but the local `ssh` call still hangs ~20s+).
    # We never need this SSH process's own exit code; we only need the
    # server to become reachable, which we poll for separately below.
    argv = ssh_base(remote) + [script]
    proc = subprocess.Popen(
        argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if server_is_up(remote):
            proc.poll()  # reap if it has in fact exited by now; harmless otherwise
            return
        time.sleep(1.0)
    proc.poll()
    raise RuntimeError(f"server did not become reachable within {timeout}s")


def _server_pids_script(remote: RemoteArgs) -> str:
    # Match `server -C <absolute config path>`: present in BOTH the
    # `clickhouse-watchdog` process and its `clickhouse server` child, and
    # in nothing else -- in particular NOT in this tool's own driver
    # process (whose command line carries the config path only as a
    # `--remote-config <path>` argument; matching on the bare path killed
    # the driver itself when driver and server shared a host), and NOT in
    # an unrelated server started from a different config.
    #
    # The `[s]erver` bracket is load-bearing, not decoration: every carrier
    # of this script's text -- the remote shell running it, its ssh
    # transport, and (when driver and server share a host) the local ssh
    # client process -- has the PATTERN string in its command line, and a
    # plain pattern therefore matched and `kill -9`ed the kill machinery
    # itself, including a just-issued launch's ssh (measured: the relaunch
    # ssh died rc=-9 and the server never came up). As a regex, `[s]erver`
    # still matches the literal `server ...` in the watchdog/server
    # command lines, but the literal `[s]erver` text in any carrier's
    # command line does not match the regex, so the machinery can never
    # target itself. `grep -vw $$` stays as belt-and-suspenders for the
    # script's own shell.
    pattern = shlex.quote(f"[s]erver -C {remote.config}")
    return f"pgrep -f {pattern} 2>/dev/null | grep -vw $$ || true"


def ensure_server_stopped(remote: RemoteArgs, *, timeout: float = 60.0) -> None:
    script = (
        f"PIDS=$({_server_pids_script(remote)}); "
        '[ -n "$PIDS" ] && kill -9 $PIDS 2>/dev/null; '
        f"for i in $(seq 1 {int(timeout)}); do "
        f"PIDS=$({_server_pids_script(remote)}); "
        '[ -z "$PIDS" ] && break; '
        'kill -9 $PIDS 2>/dev/null; sleep 1; done; echo stopped'
    )
    run_ssh(remote, script, timeout=timeout + 10)


def stop_server(remote: RemoteArgs, *, timeout: float = 60.0) -> None:
    ensure_server_stopped(remote, timeout=timeout)


# --------------------------------------------------------------------------
# SQL builders: keys_store
# --------------------------------------------------------------------------


def _numeric_key_expr(rank_col: str, ncols: int, seed: int, col_index: int) -> str:
    mix = (seed + col_index * 0x9E3779B97F4A7C15) & UINT64_MAX
    return f"intHash64(bitXor(toUInt64({rank_col}), toUInt64({mix})))"


def _string_key_expr(rank_col: str, strlen: int, seed: int) -> str:
    """Deterministic fixed-length (byte-exact) String derived from rank.

    Concatenates ceil(strlen/8) independent 8-byte hash chunks and truncates
    to the exact byte length, so every row is EXACTLY `strlen` bytes.
    """
    chunks = (strlen + 7) // 8
    parts = []
    for i in range(chunks):
        mix = (seed + i * 0xC2B2AE3D27D4EB4F) & UINT64_MAX
        parts.append(
            f"reinterpretAsFixedString(intHash64(bitXor(toUInt64({rank_col}), toUInt64({mix}))))"
        )
    concat = "concat(" + ", ".join(parts) + ")" if len(parts) > 1 else parts[0]
    return f"substring(toString({concat}), 1, {strlen})"


def key_id_salt(key_id: str) -> int:
    """Per-key-config salt so distinct `K*` configs never coincidentally
    share a keyspace (e.g. K1's first UInt64 column would otherwise be
    byte-identical to K2's, since both derive column 0 the same way)."""
    digest = hashlib.sha256(key_id.encode()).digest()
    return int.from_bytes(digest[:8], "big")


def key_value_exprs(cfg: KeyConfig, rank_col: str, seed: int) -> list[str]:
    salted_seed = (seed ^ key_id_salt(cfg.key_id)) & UINT64_MAX
    if cfg.kind in ("numeric", "nullable_numeric"):
        return [_numeric_key_expr(rank_col, cfg.ncols, salted_seed, i) for i in range(cfg.ncols)]
    return [_string_key_expr(rank_col, cfg.strlen, salted_seed)]


def keys_store_table_name(key_id: str) -> str:
    return f"keys_store.{key_id.lower()}"


def keys_store_create_sql(cfg: KeyConfig) -> str:
    cols = ", ".join(f"{name} {cfg.column_type(nullable_wrap=False)}" for name in cfg.key_columns)
    table = keys_store_table_name(cfg.key_id)
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (rank UInt64, {cols}) "
        f"ENGINE = MergeTree ORDER BY rank "
        f"COMMENT 'N_max={cfg.n_max} key_id={cfg.key_id} kind={cfg.kind}';"
    )


def keys_store_fill_sql(cfg: KeyConfig) -> str:
    """Single INSERT covering both the hit domain [0,N_max) and the disjoint
    miss domain [N_max, 2*N_max), switching the hash seed at the boundary so
    the two domains are (with overwhelming probability) disjoint sets of
    keys -- verified empirically in `selftest` via an anti-join count."""
    table = keys_store_table_name(cfg.key_id)
    n_max = cfg.n_max
    hit_exprs = key_value_exprs(cfg, "rank", KEY_SEED_HIT)
    # Miss-domain rows reuse `rank - N_max` as the local rank so the two
    # domains are generated by structurally the same function family with a
    # different key seed (not just a different additive offset, which could
    # otherwise correlate under a linear-ish hash).
    miss_exprs = key_value_exprs(cfg, "(rank - toUInt64(" + str(n_max) + "))", KEY_SEED_MISS)
    cols_sql = ", ".join(
        f"if(rank < {n_max}, {hit}, {miss}) AS {name}"
        for name, hit, miss in zip(cfg.key_columns, hit_exprs, miss_exprs)
    )
    return (
        f"INSERT INTO {table} SELECT number AS rank, {cols_sql} "
        f"FROM numbers({2 * n_max}) "
        f"SETTINGS max_insert_threads = 96, max_threads = 96;"
    )


def keys_store_checksum_sql(cfg: KeyConfig) -> str:
    table = keys_store_table_name(cfg.key_id)
    key_expr = ", ".join(cfg.key_columns)
    return (
        f"SELECT count() AS row_count, "
        f"sum(cityHash64({key_expr})) AS checksum "
        f"FROM {table} FORMAT JSONEachRow"
    )


# --------------------------------------------------------------------------
# SQL builders: Memory build/probe tables
# --------------------------------------------------------------------------

BUILD_TABLE = "bench.build_t"
PROBE_TABLE = "bench.probe_t"
# `max_threads`/`max_insert_threads` = 1 is deliberate defense-in-depth for
# the "duplicate keys are D rows apart" occurrence-major invariant: with a
# multi-threaded INSERT pipeline there is no documented guarantee that the
# Memory-table insert executor appends blocks from a sorted source stream in
# that stream's order across insert threads. (The read side has the same
# hazard for verification: reading the table back with more than one
# thread scrambles `rowNumberInAllBlocks()` relative to storage order --
# measured directly, see `selftest --check-correctness`'s occurrence-major
# check, which reads with `max_threads=1` for exactly this reason.) Fills
# are cheap (no heavy I/O, small blocks) so the single-thread cost here is
# acceptable.
FILL_SETTINGS = (
    "SETTINGS min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, "
    "max_block_size = 57344, max_threads = 1, max_insert_threads = 1"
)

PAYLOAD_BYTES_TO_COLS = {0: 0, 8: 1, 16: 2, 32: 4, 64: 8}


def payload_columns_ddl(prefix: str, payload_bytes: int) -> list[tuple[str, str]]:
    count = PAYLOAD_BYTES_TO_COLS[payload_bytes]
    return [(f"{prefix}{i}", "UInt64") for i in range(count)]


def payload_select_exprs(prefix: str, payload_bytes: int, rank_col: str, seed: int) -> list[str]:
    count = PAYLOAD_BYTES_TO_COLS[payload_bytes]
    exprs = []
    for i in range(count):
        mix = (seed + i * 0x2545F4914F6CDD1D) & UINT64_MAX
        exprs.append(f"intHash64(bitXor(toUInt64({rank_col}), toUInt64({mix}))) AS {prefix}{i}")
    return exprs


def mem_table_create_sql(
    table: str, cfg: KeyConfig, payload_bytes: int, prefix: str, *, null_wrap: bool
) -> str:
    key_cols = [(name, cfg.column_type(nullable_wrap=null_wrap)) for name in cfg.key_columns]
    payload_cols = payload_columns_ddl(prefix, payload_bytes)
    all_cols = key_cols + payload_cols
    cols_sql = ", ".join(f"{name} {type_}" for name, type_ in all_cols)
    return f"DROP TABLE IF EXISTS {table}; CREATE TABLE {table} ({cols_sql}) ENGINE = Memory SETTINGS compress = false;"


def _null_wrap_select(cfg: KeyConfig, raw_exprs: list[str], nulls_pct: int, rank_col: str) -> list[str]:
    if not cfg.is_nullable:
        return raw_exprs
    if nulls_pct <= 0:
        return [f"CAST({expr} AS Nullable({'String' if cfg.kind == 'nullable_string' else 'UInt64'}))" for expr in raw_exprs]
    if nulls_pct != 10:
        raise ValueError("only nulls_pct in (0, 10) is implemented")
    base_type = "String" if cfg.kind == "nullable_string" else "UInt64"
    return [
        f"if(({rank_col}) % 10 = 0, NULL, CAST({expr} AS Nullable({base_type})))" for expr in raw_exprs
    ]


def build_fill_statements(
    cfg: KeyConfig, D: int, m_b: int, bp: int, nulls_pct: int, skew_s: int = 0
) -> list[str]:
    key_exprs = key_value_exprs(cfg, "rank", KEY_SEED_HIT)
    key_exprs = _null_wrap_select(cfg, key_exprs, nulls_pct, "rank")
    key_select = ", ".join(f"{expr} AS {name}" for expr, name in zip(key_exprs, cfg.key_columns))
    payload_exprs = payload_select_exprs("b_p", bp, "rank", BUILD_PAYLOAD_SEED)
    payload_select = (", " + ", ".join(payload_exprs)) if payload_exprs else ""
    src = keys_store_table_name(cfg.key_id)
    select = (
        f"SELECT {key_select}{payload_select} FROM {src} "
        f"WHERE rank < {D} ORDER BY rank"
    )
    stmts = [f"INSERT INTO {BUILD_TABLE} {select} {FILL_SETTINGS};" for _ in range(m_b)]
    if skew_s > 0:
        # Build-side skew: `skew_s` extra rows all carrying the rank-0 key,
        # appended AFTER the occurrence-major passes. The key expressions
        # are constants (rank fixed to 0), so all S rows share one key;
        # payloads still vary with `number` (values are irrelevant to the
        # join). These S rows sit adjacent to each other by construction --
        # a documented deviation from occurrence-major spacing, which only
        # ever applies to the skew key. Requires h=1.0/nulls=0 (enforced by
        # `expected_output_rows`), keeping the closed form exact:
        # output = m_p * (D*m_b + S).
        if nulls_pct != 0:
            raise ValueError("skew_s requires nulls_pct=0")
        skew_key_exprs = key_value_exprs(cfg, "0", KEY_SEED_HIT)
        skew_key_exprs = _null_wrap_select(cfg, skew_key_exprs, 0, "0")
        skew_key_select = ", ".join(
            f"{expr} AS {name}" for expr, name in zip(skew_key_exprs, cfg.key_columns)
        )
        skew_payload_exprs = payload_select_exprs("b_p", bp, "number", BUILD_PAYLOAD_SEED ^ 0x5)
        skew_payload_select = (", " + ", ".join(skew_payload_exprs)) if skew_payload_exprs else ""
        stmts.append(
            f"INSERT INTO {BUILD_TABLE} SELECT {skew_key_select}{skew_payload_select} "
            f"FROM numbers({skew_s}) {FILL_SETTINGS};"
        )
    return stmts


def probe_fill_statements(
    cfg: KeyConfig, D: int, m_p: int, h: decimal.Decimal, pp: int, n_max: int, nulls_pct: int
) -> list[str]:
    hit_count = decimal_int_product(D, h)
    miss_count = D - hit_count
    key_exprs_hit = key_value_exprs(cfg, "rank", KEY_SEED_HIT)
    key_exprs_hit = _null_wrap_select(cfg, key_exprs_hit, nulls_pct, "rank")
    key_select_hit = ", ".join(f"{e} AS {n}" for e, n in zip(key_exprs_hit, cfg.key_columns))
    payload_exprs_hit = payload_select_exprs("p_p", pp, "rank", PROBE_PAYLOAD_SEED)
    payload_select_hit = (", " + ", ".join(payload_exprs_hit)) if payload_exprs_hit else ""
    src = keys_store_table_name(cfg.key_id)

    parts = []
    if hit_count > 0:
        parts.append(
            f"SELECT {key_select_hit}{payload_select_hit} FROM {src} "
            f"WHERE rank < {hit_count} ORDER BY rank"
        )
    if miss_count > 0:
        rank_col = f"(rank - toUInt64({n_max}))"
        # Miss rows are read from this same store table's own miss-domain
        # rows (rank in [n_max, n_max+miss_count)); their stored key columns
        # are used directly (no re-derivation), guaranteeing bit-identical
        # miss keys across cells/algorithms.
        payload_exprs_miss = payload_select_exprs(
            "p_p", pp, rank_col, PROBE_PAYLOAD_SEED ^ 0x1
        )
        payload_select_miss = (", " + ", ".join(payload_exprs_miss)) if payload_exprs_miss else ""
        key_cols_miss = ", ".join(cfg.key_columns)
        miss_key_select = key_cols_miss
        if cfg.is_nullable and nulls_pct == 10:
            base_type = "String" if cfg.kind == "nullable_string" else "UInt64"
            miss_key_select = ", ".join(
                f"if(({rank_col}) % 10 = 0, NULL, CAST({name} AS Nullable({base_type}))) AS {name}"
                for name in cfg.key_columns
            )
        elif cfg.is_nullable:
            base_type = "String" if cfg.kind == "nullable_string" else "UInt64"
            miss_key_select = ", ".join(
                f"CAST({name} AS Nullable({base_type})) AS {name}" for name in cfg.key_columns
            )
        parts.append(
            f"SELECT {miss_key_select}{payload_select_miss} FROM {src} "
            f"WHERE rank >= {n_max} AND rank < {n_max + miss_count} ORDER BY rank"
        )
    select = " UNION ALL ".join(f"({p})" for p in parts) if len(parts) > 1 else parts[0]
    stmt = f"INSERT INTO {PROBE_TABLE} SELECT * FROM ({select}) {FILL_SETTINGS};"
    return [stmt for _ in range(m_p)]


def decimal_int_product(base: int, factor_str: str) -> int:
    factor = decimal.Decimal(factor_str)
    result = decimal.Decimal(base) * factor
    if result != result.to_integral_value():
        raise ValueError(f"{base} * {factor_str} is not an exact integer")
    return int(result)


def expected_output_rows(
    D: int, m_b: int, m_p: int, h: str, nulls_pct: int, skew_s: int = 0
) -> int:
    hit_count = decimal_int_product(D, h)
    if nulls_pct == 0:
        matching_hit = hit_count
    elif nulls_pct == 10:
        if hit_count % 10 != 0:
            raise ValueError("nulls_pct=10 requires D*h divisible by 10 for an exact closed form")
        matching_hit = hit_count - hit_count // 10
    else:
        raise ValueError("only nulls_pct in (0, 10) is implemented")
    base = matching_hit * m_p * m_b
    if skew_s > 0:
        # Skew cells require full hit rate and no NULLs so the single skewed
        # key (rank 0) appears exactly once per probe pass: every pass adds
        # `skew_s` extra matches on top of the uniform D*m_b.
        if h != "1.0" or nulls_pct != 0:
            raise ValueError("skew_s requires h='1.0' and nulls_pct=0")
        base += skew_s * m_p
    return base


# --------------------------------------------------------------------------
# Join query
# --------------------------------------------------------------------------


def join_settings(algorithm: str, threads: int, *, profiled: bool, log_comment: str) -> str:
    parts = [
        f"join_algorithm = '{algorithm}'",
        f"max_threads = {threads}",
        "query_plan_join_swap_table = false",
        "enable_analyzer = 1",
        "enable_join_runtime_filters = 0",
        "max_bytes_before_external_join = 0",
        "max_bytes_ratio_before_external_join = 0",
        "max_memory_usage = 300000000000",
        "max_execution_time = 600",
        f"log_comment = '{log_comment}'",
    ]
    if profiled:
        parts += [
            "query_profiler_real_time_period_ns = 2000000",
            "query_profiler_cpu_time_period_ns = 2000000",
            "log_processors_profiles = 1",
        ]
    else:
        parts += [
            "query_profiler_real_time_period_ns = 0",
            "query_profiler_cpu_time_period_ns = 0",
            "log_processors_profiles = 0",
        ]
    return "SETTINGS " + ", ".join(parts)


def join_query_sql(cfg: KeyConfig, algorithm: str, threads: int, *, profiled: bool, log_comment: str) -> str:
    on_clause = " AND ".join(f"l.{name} = r.{name}" for name in cfg.key_columns)
    settings = join_settings(algorithm, threads, profiled=profiled, log_comment=log_comment)
    projection = "count() AS row_count, sum(cityHash64(*)) AS checksum"
    return (
        f"SELECT {projection} FROM {PROBE_TABLE} AS l "
        f"INNER JOIN {BUILD_TABLE} AS r ON {on_clause} "
        f"{settings} FORMAT JSONEachRow"
    )


# --------------------------------------------------------------------------
# Plan generation
# --------------------------------------------------------------------------


def make_cell(
    *, D: int, key: str, m_b: int, m_p: int, h: str, bp: int, pp: int, threads: int,
    nulls_pct: int = 0, skew_s: int = 0, runs: int = 5, rep: int = 0,
    group: str, note: str = "",
) -> dict:
    cell_id = (
        f"D{D}_{key}_mb{m_b}_mp{m_p}_h{h}_bp{bp}_pp{pp}_T{threads}"
        + (f"_nulls{nulls_pct}" if nulls_pct else "")
        + (f"_skew{skew_s}" if skew_s else "")
        + (f"_rep{rep}" if rep else "")
    )
    if skew_s and (h != "1.0" or nulls_pct != 0):
        raise ValueError(f"skew cell {cell_id} requires h='1.0' and nulls_pct=0")
    cfg = KEY_CONFIGS[key]
    hit = decimal_int_product(D, h)
    if D > cfg.n_max:
        raise ValueError(f"cell {cell_id}: D={D} exceeds {key} N_max={cfg.n_max}")
    if (D - hit) > cfg.n_max:
        raise ValueError(f"cell {cell_id}: miss count exceeds {key} miss domain")
    return {
        "cell_id": cell_id,
        "D": D,
        "key": key,
        "m_b": m_b,
        "m_p": m_p,
        "h": h,
        "bp": bp,
        "pp": pp,
        "threads": threads,
        "nulls_pct": nulls_pct,
        "skew_s": skew_s,
        "runs": runs,
        "rep": rep,
        "group": group,
        "note": note,
    }


def generate_plan() -> list[dict]:
    cells: dict[str, dict] = {}

    def add(cell: dict) -> None:
        existing = cells.get(cell["cell_id"])
        if existing is None:
            cell["groups"] = [cell.pop("group")]
            cells[cell["cell_id"]] = cell
        else:
            group = cell["group"]
            if group not in existing["groups"]:
                existing["groups"].append(group)
            if cell.get("note") and cell["note"] not in existing.get("note", ""):
                existing["note"] = (existing.get("note", "") + ";" + cell["note"]).strip(";")

    d_ladder = [2_000_000, 8_000_000, 64_000_000, 128_000_000, 256_000_000, 512_000_000]
    key_ladder = [f"K{i}" for i in range(1, 10)]

    # 1. Backbone.
    for T in (8, 16, 64, 96):
        add(make_cell(**ANCHOR, threads=T, group="backbone", note="anchor"))
        for D in d_ladder:
            add(make_cell(**{**ANCHOR, "D": D}, threads=T, group="backbone", note="D-ladder"))
        for key in key_ladder:
            add(make_cell(**{**ANCHOR, "key": key}, threads=T, group="backbone", note="key-ladder"))

    # 2. OFAT from the anchor, h=0.9, T in {16, 96}.
    for T in (16, 96):
        base = {**ANCHOR, "h": "0.9"}
        add(make_cell(**base, threads=T, group="ofat", note="ofat-base(h=0.9)"))
        for bp in (0, 16, 32, 64):
            add(make_cell(**{**base, "bp": bp}, threads=T, group="ofat", note="ofat-bp"))
        for pp in (0, 16, 32, 64):
            add(make_cell(**{**base, "pp": pp}, threads=T, group="ofat", note="ofat-pp"))
        for m_b in (4, 8, 16):
            add(make_cell(**{**base, "m_b": m_b}, threads=T, group="ofat", note="ofat-mb"))
        for m_p in (4, 8, 16):
            add(make_cell(**{**base, "m_p": m_p}, threads=T, group="ofat", note="ofat-mp"))

    # 3. Interaction probes, T=96 unless noted.
    interactions = [
        (dict(D=512_000_000, key="K7"), 96, "P1"),
        (dict(D=512_000_000, pp=64), 96, "P2"),
        (dict(D=128_000_000, pp=64), 96, "P3"),
        (dict(D=2_000_000, pp=64), 96, "P4"),
        (dict(D=2_000_000, pp=64), 8, "P4@T8"),
        (dict(m_b=16, pp=64), 96, "P5"),
        (dict(m_b=16, m_p=16), 96, "P6"),
        (dict(h="0.9", key="K9"), 96, "P7(+10%nulls)"),
        (dict(h="0.9", D=256_000_000), 96, "P8"),
        (dict(key="K3", m_b=8), 96, "P9"),
        (dict(D=2_000_000), 96, "P10"),
    ]
    for overrides, T, tag in interactions:
        nulls_pct = 10 if tag.startswith("P7") else 0
        add(make_cell(**{**ANCHOR, **overrides}, threads=T, nulls_pct=nulls_pct, group="interaction", note=tag))

    ordered = sorted(
        cells.values(),
        key=lambda c: (c["groups"][0], c["threads"], c["D"], c["key"], c["cell_id"]),
    )
    return ordered


# Relative per-row cost weights by key config, from phase-1 medians at the
# D=32M anchor (K0 T8 = 176ms defines 1.0; K3 = 1612ms ~ 9x etc.). Only used
# to balance shard assignment -- accuracy beyond rank order is unnecessary.
KEY_COST_WEIGHT = {
    "K0": 1.0, "K1": 2.2, "K2": 4.2, "K3": 9.0, "K4": 2.1, "K5": 2.4,
    "K6": 3.0, "K7": 3.7, "K8": 1.1, "K9": 2.4,
}


def cell_cost_estimate(cell: dict) -> float:
    """Deterministic relative cost for shard balancing: touched rows
    (build + probe + skew + output replication) x key weight x run count."""
    D, m_b, m_p = cell["D"], cell["m_b"], cell["m_p"]
    skew = cell.get("skew_s", 0)
    rows = D * (m_b + m_p) + skew * (1 + m_p)
    executions = WARMUP_RUNS + int(cell.get("runs", 5)) + 1
    return rows * KEY_COST_WEIGHT[cell["key"]] * executions


def generate_plan_phase2(shards: int = 4) -> list[dict]:
    """Phase-2 plan: ~280 unique cells in 11 groups (A-J, R), designed to
    bound the regret of replacing `parallel_hash` with `partitioned_hash`.
    See the generated report for the
    findings each group probes. Cells are deduplicated by cell_id (a cell
    may carry several group tags) and assigned to `shards` instances by
    greedy LPT on `cell_cost_estimate` (deterministic)."""
    cells: dict[str, dict] = {}

    def add(cell: dict) -> None:
        existing = cells.get(cell["cell_id"])
        if existing is None:
            cell["groups"] = [cell.pop("group")]
            cells[cell["cell_id"]] = cell
        else:
            group = cell["group"]
            if group not in existing["groups"]:
                existing["groups"].append(group)
            if cell.get("note") and cell["note"] not in existing.get("note", ""):
                existing["note"] = (existing.get("note", "") + ";" + cell["note"]).strip(";")
            # A replication-run-count upgrade wins over the default.
            if cell.get("runs", 5) > existing.get("runs", 5):
                existing["runs"] = cell["runs"]

    M = 1_000_000
    base = dict(m_b=1, m_p=1, h="1.0", bp=8, pp=8)
    t_ladder_fine = (2, 4, 8, 16, 24, 32, 48, 64, 96)

    # A -- loss-region cartography: K1/K2 (the phase-1 weak spot) over a
    # fine T ladder and a D ladder; K3 (`hashed`) over
    # the same T ladder as the mechanism control.
    for key in ("K1", "K2"):
        for T in t_ladder_fine:
            for D in (8 * M, 32 * M, 128 * M):
                add(make_cell(**{**base, "D": D, "key": key}, threads=T, group="A", note="loss-region"))
    for T in t_ladder_fine:
        add(make_cell(**{**base, "D": 32 * M, "key": "K3"}, threads=T, group="A", note="hashed-control"))

    # B -- mechanism isolation at the weak point.
    for T in (4, 8, 16):
        for key in ("K0", "K1", "K2", "K3"):
            for D in (8 * M, 32 * M):
                add(make_cell(**{**base, "D": D, "key": key}, threads=T, group="B", note="width-grid"))
    for overrides, note in (
        (dict(bp=0, pp=0), "pure-lookup"),
        (dict(pp=32), "pp32"),
        (dict(pp=64), "pp64"),
        (dict(m_p=4), "mp4"),
        (dict(m_p=16), "mp16"),
        (dict(m_b=4), "mb4"),
        (dict(h="0.5"), "h0.5"),
        (dict(h="0.05"), "h0.05"),
    ):
        add(make_cell(**{**base, "D": 32 * M, "key": "K2", **overrides}, threads=8, group="B", note=f"K2-weakpoint-{note}"))
    add(make_cell(**{**base, "D": 32 * M, "key": "K1", "pp": 64}, threads=8, group="B", note="K1-weakpoint-pp64"))
    add(make_cell(**{**base, "D": 32 * M, "key": "K1", "h": "0.5"}, threads=8, group="B", note="K1-weakpoint-h0.5"))
    for key in ("K5", "K6"):
        for T in (2, 4):
            add(make_cell(**{**base, "D": 32 * M, "key": key}, threads=T, group="B", note="string-lowT"))

    # C -- dimension-join shapes: small build side, probe 16-256x larger.
    for D in (65536, 262144, 1 * M, 4 * M):
        for m_p in (16, 64, 256):
            if D * m_p > 1_100_000_000:
                continue
            for T in (8, 16, 96):
                add(make_cell(**{**base, "D": D, "key": "K0", "m_p": m_p}, threads=T, group="C", note="dimension"))
    for D in (262144, 1 * M):
        for m_p in (64, 256):
            for T in (16, 96):
                add(make_cell(**{**base, "D": D, "key": "K5", "m_p": m_p}, threads=T, group="C", note="dimension-string"))
    for T in (8, 96):
        add(make_cell(**{**base, "D": 1 * M, "key": "K2", "m_p": 64}, threads=T, group="C", note="dimension-weakkey"))

    # D -- hit-rate ladder.
    for h in ("0.75", "0.5", "0.25", "0.05"):
        for key in ("K0", "K1", "K2", "K5", "K7"):
            for T in (8, 96):
                add(make_cell(**{**base, "D": 32 * M, "key": key, "h": h}, threads=T, group="D", note="hit-ladder"))

    # E -- build-side skew (S extra rows of one hot key; h=1.0 by design).
    for S in (8 * M, 32 * M, 128 * M):
        for T in (8, 16, 96):
            add(make_cell(**{**base, "D": 32 * M, "key": "K0"}, skew_s=S, threads=T, group="E", note="skew"))
    for S in (8 * M, 32 * M):
        add(make_cell(**{**base, "D": 32 * M, "key": "K2"}, skew_s=S, threads=8, group="E", note="skew-weakkey"))
        add(make_cell(**{**base, "D": 32 * M, "key": "K5"}, skew_s=S, threads=96, group="E", note="skew-string"))

    # F -- memory worst-case mapping (phase-1 ceiling: 1.86 at K3 mb8 T96).
    for key in ("K3", "K7"):
        for m_b in (2, 4, 8, 16):
            add(make_cell(**{**base, "D": 32 * M, "key": key, "m_b": m_b}, threads=96, group="F", note="mem-mb"))
        add(make_cell(**{**base, "D": 8 * M, "key": key, "m_b": 16}, threads=96, group="F", note="mem-mb-smallD"))
    for bp in (16, 64):
        add(make_cell(**{**base, "D": 32 * M, "key": "K3", "bp": bp}, threads=96, group="F", note="mem-bp"))
    for pp in (16, 64):
        add(make_cell(**{**base, "D": 32 * M, "key": "K7", "pp": pp}, threads=96, group="F", note="mem-pp"))
    add(make_cell(**{**base, "D": 32 * M, "key": "K6", "m_b": 16}, threads=96, group="F", note="mem-mb"))
    add(make_cell(**{**base, "D": 32 * M, "key": "K9", "m_b": 16}, threads=96, group="F", note="mem-mb"))
    add(make_cell(**{**base, "D": 128 * M, "key": "K7", "m_b": 4}, threads=96, group="F", note="mem-scale"))

    # G -- replication of the decision cells: 3 independent instances each,
    # 15 timed runs (distinct cell_ids so the resumable sweep treats them
    # as separate cells with their own server lifecycles).
    for key, T in (("K2", 8), ("K2", 16), ("K1", 8), ("K0", 96)):
        for rep in (1, 2, 3):
            add(make_cell(**{**base, "D": 32 * M, "key": key}, threads=T, runs=15, rep=rep, group="G", note="replication"))

    # H -- scale ladder for every non-K0 key family (earlier sweeps pinned all of
    # them at D=32M; N_max is now 128M for K1-K6/K8/K9).
    for key in ("K1", "K2", "K5", "K8"):
        for D in (64 * M, 128 * M):
            for T in (8, 96):
                add(make_cell(**{**base, "D": D, "key": key}, threads=T, group="H", note="scale"))
    for key in ("K3", "K4", "K6", "K9"):
        for T in (8, 96):
            add(make_cell(**{**base, "D": 128 * M, "key": key}, threads=T, group="H", note="scale"))
    add(make_cell(**{**base, "D": 128 * M, "key": "K7"}, threads=8, group="H", note="scale"))
    for T in (8, 16):
        add(make_cell(**{**base, "D": 512 * M, "key": "K7"}, threads=T, group="H", note="scale-512M"))

    # I -- low/mid-T ladder for the string/nullable families.
    for key in ("K5", "K7", "K8", "K9"):
        for T in (2, 4, 32):
            add(make_cell(**{**base, "D": 32 * M, "key": key}, threads=T, group="I", note="family-T-ladder"))

    # J -- payload corner matrix at scale and at the weak key.
    for bp, pp in ((0, 0), (64, 64), (0, 64), (64, 0)):
        for D, key in ((128 * M, "K0"), (32 * M, "K2")):
            for T in (16, 96):
                add(make_cell(**{**base, "D": D, "key": key, "bp": bp, "pp": pp}, threads=T, group="J", note="payload-corners"))

    # R -- cross-instance sentinels: exact phase-1 re-runs (same cell_ids).
    add(make_cell(**ANCHOR, threads=16, group="R", note="sentinel-anchor"))
    add(make_cell(**ANCHOR, threads=96, group="R", note="sentinel-anchor"))
    add(make_cell(**{**base, "D": 512 * M, "key": "K0"}, threads=96, group="R", note="sentinel-512M"))
    add(make_cell(**{**base, "D": 128 * M, "key": "K0"}, threads=96, group="R", note="sentinel-128M"))
    add(make_cell(**{**base, "D": 32 * M, "key": "K3"}, threads=8, group="R", note="sentinel-K3"))
    add(make_cell(**{**base, "D": 32 * M, "key": "K7"}, threads=96, group="R", note="sentinel-K7"))
    add(make_cell(**{**base, "D": 2 * M, "key": "K0"}, threads=8, group="R", note="sentinel-jit"))
    add(make_cell(**{**base, "D": 2 * M, "key": "K0", "pp": 64}, threads=8, group="R", note="sentinel-jit"))

    ordered = sorted(cells.values(), key=lambda c: c["cell_id"])

    # Deterministic LPT: heaviest cell first onto the least-loaded shard.
    loads = [0.0] * shards
    for cell in sorted(ordered, key=lambda c: (-cell_cost_estimate(c), c["cell_id"])):
        shard = loads.index(min(loads))
        cell["shard"] = shard
        loads[shard] += cell_cost_estimate(cell)
    return ordered


def build_plan(shards: int = 1) -> list[dict]:
    """THE plan: every distinct cell of the sweep, deduplicated by
    cell_id into one 347-cell plan - 23 cells appear in both sources; their
    group tags and notes are merged and the larger run count wins - and
    assigned to `shards` instances by deterministic LPT on the cost
    estimate."""
    cells: dict[str, dict] = {}
    for source in (generate_plan_phase2(shards=1), generate_plan()):
        for cell in source:
            c = dict(cell)
            c.pop("shard", None)
            existing = cells.get(c["cell_id"])
            if existing is None:
                cells[c["cell_id"]] = c
                continue
            for group in c["groups"]:
                if group not in existing["groups"]:
                    existing["groups"].append(group)
            if c.get("note") and c["note"] not in existing.get("note", ""):
                existing["note"] = (existing.get("note", "") + ";" + c["note"]).strip(";")
            if c.get("runs", 5) > existing.get("runs", 5):
                existing["runs"] = c["runs"]

    ordered = sorted(cells.values(), key=lambda c: c["cell_id"])

    # Deterministic LPT: heaviest cell first onto the least-loaded shard.
    loads = [0.0] * shards
    for cell in sorted(ordered, key=lambda c: (-cell_cost_estimate(c), c["cell_id"])):
        shard = loads.index(min(loads))
        cell["shard"] = shard
        loads[shard] += cell_cost_estimate(cell)
    return ordered


def plan_command(args: argparse.Namespace) -> int:
    shards = getattr(args, "shards", 4)
    plan = build_plan(shards=shards)
    if getattr(args, "shard_summary", False):
        by_shard: dict[int, list[dict]] = {}
        for c in plan:
            by_shard.setdefault(c.get("shard", 0), []).append(c)
        for shard in sorted(by_shard):
            group_cells = by_shard[shard]
            keys = sorted({c["key"] for c in group_cells})
            cost = sum(cell_cost_estimate(c) for c in group_cells)
            print(
                f"shard {shard}: cells={len(group_cells)} est_cost={cost:.3e} "
                f"required_keys={','.join(keys)}"
            )
        print(f"total unique cells={len(plan)}")
        return 0
    out = json.dumps(plan, indent=2)
    if args.out:
        pathlib.Path(args.out).write_text(out + "\n")
        print(f"Wrote {len(plan)} cells to {args.out}")
    else:
        print(out)
    group_counts: dict[str, int] = {}
    for c in plan:
        for g in c["groups"]:
            group_counts[g] = group_counts.get(g, 0) + 1
    print(
        f"# total unique cells={len(plan)} group members={group_counts} "
        "(a cell may belong to more than one group, so these do not simply add to the total)",
        file=sys.stderr,
    )
    return 0


# --------------------------------------------------------------------------
# prepare-keys
# --------------------------------------------------------------------------


def prepare_keys_command(args: argparse.Namespace) -> int:
    remote = remote_args_from_ns(args)
    start_server(remote)
    run_remote_sql(remote, "CREATE DATABASE IF NOT EXISTS keys_store;")
    key_ids = args.keys.split(",") if args.keys else list(KEY_CONFIGS)
    fp_path = pathlib.Path(args.local_root) / "keys" / "fingerprints.json"
    fp_path.parent.mkdir(parents=True, exist_ok=True)
    fingerprints: dict[str, dict] = {}
    if fp_path.exists():
        fingerprints = json.loads(fp_path.read_text())

    for key_id in key_ids:
        cfg = KEY_CONFIGS[key_id]
        table = keys_store_table_name(key_id)
        expected_rows = 2 * cfg.n_max
        existing = fingerprints.get(key_id)
        current_count = None
        rc, _, _ = run_remote_sql(remote, f"EXISTS TABLE {table} FORMAT TSV")
        exists_rows = run_remote_sql_json(remote, f"SELECT count() AS n FROM system.tables WHERE database='keys_store' AND name='{key_id.lower()}' FORMAT JSONEachRow")
        table_exists = int(exists_rows[0]["n"]) > 0
        if table_exists:
            rows = run_remote_sql_json(remote, f"SELECT count() AS n FROM {table} FORMAT JSONEachRow")
            current_count = int(rows[0]["n"])
        if (
            table_exists
            and current_count == expected_rows
            and existing is not None
            and existing.get("row_count") == expected_rows
            and existing.get("n_max") == cfg.n_max
        ):
            print(f"{key_id}: SKIP (already valid: {expected_rows} rows, fingerprint on file)")
            continue

        print(f"{key_id}: (re)creating, N_max={cfg.n_max}, expecting {expected_rows} rows ...")
        rc, _, stderr = run_remote_sql(remote, f"DROP TABLE IF EXISTS {table};")
        require_ok(rc, stderr, f"{key_id} drop")
        rc, _, stderr = run_remote_sql(remote, keys_store_create_sql(cfg), timeout=60)
        require_ok(rc, stderr, f"{key_id} create")
        t0 = time.monotonic()
        rc, _, stderr = run_remote_sql(remote, keys_store_fill_sql(cfg), timeout=3600)
        require_ok(rc, stderr, f"{key_id} fill")
        elapsed = time.monotonic() - t0
        rows = run_remote_sql_json(remote, keys_store_checksum_sql(cfg), timeout=300)
        row_count = int(rows[0]["row_count"])
        checksum = int(rows[0]["checksum"])
        if row_count != expected_rows:
            raise RuntimeError(f"{key_id}: row count {row_count} != expected {expected_rows}")
        fingerprint = hashlib.sha256(f"{key_id}|{row_count}|{checksum}".encode()).hexdigest()
        fingerprints[key_id] = {
            "n_max": cfg.n_max,
            "row_count": row_count,
            "checksum": checksum,
            "fingerprint": fingerprint,
            "fill_seconds": elapsed,
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        fp_path.write_text(json.dumps(fingerprints, indent=2) + "\n")
        print(f"{key_id}: OK rows={row_count} checksum={checksum} fingerprint={fingerprint[:16]}... ({elapsed:.1f}s)")
    return 0


# --------------------------------------------------------------------------
# run-cell
# --------------------------------------------------------------------------


def _flush_logs(remote: RemoteArgs) -> None:
    rc, _, stderr = run_remote_sql(remote, "SYSTEM FLUSH LOGS;", timeout=60)
    require_ok(rc, stderr, "SYSTEM FLUSH LOGS")


def _export_logs_for_cell(
    remote: RemoteArgs, local_dir: pathlib.Path, log_comment_base: str, algorithm: str
) -> None:
    local_dir.mkdir(parents=True, exist_ok=True)
    # The algorithm belongs in the filter, not just the filename: the
    # log_comment layout is `<cell_id>|<run_nonce>|<algorithm>|<run tag>`,
    # and this export runs once per algorithm AFTER that algorithm's runs,
    # so a prefix filter without the algorithm would sweep up the earlier
    # algorithm's rows into the later algorithm's files (per-algorithm
    # correctness of the *measurements* was never affected -- the timing
    # query always filtered on the full `...|<algorithm>` prefix).
    like_prefix = f"{log_comment_base}|{algorithm}"
    tables = {
        "query_log": f"log_comment LIKE '{like_prefix}%'",
        "trace_log": (
            f"query_id IN (SELECT query_id FROM system.query_log "
            f"WHERE log_comment LIKE '{like_prefix}%')"
        ),
        "processors_profile_log": (
            f"query_id IN (SELECT query_id FROM system.query_log "
            f"WHERE log_comment LIKE '{like_prefix}%')"
        ),
    }
    for table, where in tables.items():
        sql = f"SELECT * FROM system.{table} WHERE {where} FORMAT JSONEachRow"
        rc, stdout, stderr = run_remote_sql(remote, sql, timeout=120)
        require_ok(rc, stderr, f"export {table} for {log_comment_base}/{algorithm}")
        (local_dir / f"{table}.{algorithm}.jsonl").write_bytes(stdout)


def _events_from_row(row: dict) -> dict[str, int]:
    pe = row.get("ProfileEvents", {}) or {}
    return {name: int(pe.get(name, 0)) for name in ALL_MAPPING_EVENTS + EXTRA_PARTITIONED_COUNTERS}


def run_one_algorithm(
    remote: RemoteArgs,
    cfg: KeyConfig,
    algorithm: str,
    threads: int,
    cell_id: str,
    run_nonce: str,
    *,
    expected_rows: int,
    runs: int = 5,
) -> dict:
    # `run_nonce` disambiguates this attempt from any earlier attempt at the
    # same `cell_id` still sitting in `system.query_log` (which persists
    # across per-cell server restarts by design -- see the
    # per-cell protocol). Without it, re-running a cell (e.g. after a bug
    # fix, or a resumed sweep re-attempting a cell that previously errored)
    # would double-count old rows matching the same `log_comment` pattern
    # (measured directly: a second run-cell attempt on an identical
    # cell_id found 10 matching query_log rows instead of the expected 5).
    log_comment_base = f"{cell_id}|{run_nonce}|{algorithm}"
    # Untimed warmups; see the WARMUP_RUNS comment for why exactly this
    # many (the JIT compilation cost must land inside the untimed window,
    # and it fires on execution #4 of a cold server).
    checksum = None
    for w in range(WARMUP_RUNS):
        warm_sql = join_query_sql(
            cfg, algorithm, threads, profiled=False, log_comment=f"{log_comment_base}|warmup{w}"
        )
        rc, stdout, stderr = run_remote_sql(remote, warm_sql, timeout=650)
        if rc != 0:
            return {"status": "INVALID", "reason": f"warmup {w} failed: {stderr}"}
        warm_row = json.loads(stdout.decode().strip().splitlines()[0])
        if int(warm_row["row_count"]) != expected_rows:
            return {
                "status": "INVALID",
                "reason": f"warmup {w} row_count {warm_row['row_count']} != expected {expected_rows}",
            }
        if checksum is None:
            checksum = int(warm_row["checksum"])
        elif int(warm_row["checksum"]) != checksum:
            return {"status": "INVALID", "reason": f"warmup {w} checksum changed across warmups"}

    # `runs` timed runs, profiling OFF.
    for i in range(runs):
        sql = join_query_sql(cfg, algorithm, threads, profiled=False, log_comment=f"{log_comment_base}|run{i}")
        rc, stdout, stderr = run_remote_sql(remote, sql, timeout=650)
        if rc != 0:
            return {"status": "INVALID", "reason": f"run {i} failed: {stderr}"}
        row = json.loads(stdout.decode().strip().splitlines()[0])
        if int(row["row_count"]) != expected_rows or int(row["checksum"]) != checksum:
            return {
                "status": "INVALID",
                "reason": (
                    f"run {i} mismatch: row_count={row['row_count']} (expected {expected_rows}) "
                    f"checksum={row['checksum']} (warmup {checksum})"
                ),
            }

    # 1 profiled run.
    prof_sql = join_query_sql(cfg, algorithm, threads, profiled=True, log_comment=f"{log_comment_base}|profiled")
    rc, stdout, stderr = run_remote_sql(remote, prof_sql, timeout=650)
    if rc != 0:
        return {"status": "INVALID", "reason": f"profiled run failed: {stderr}"}
    prof_row = json.loads(stdout.decode().strip().splitlines()[0])
    if int(prof_row["row_count"]) != expected_rows or int(prof_row["checksum"]) != checksum:
        return {"status": "INVALID", "reason": "profiled run correctness mismatch"}

    _flush_logs(remote)
    ql_rows = run_remote_sql_json(
        remote,
        (
            "SELECT query_duration_ms, memory_usage, ProfileEvents, log_comment, query_id "
            f"FROM system.query_log WHERE log_comment LIKE '{log_comment_base}%' AND type = 'QueryFinish' "
            "ORDER BY log_comment FORMAT JSONEachRow"
        ),
        timeout=60,
    )
    # Exact-suffix matching (not substring): '|run1' must not match '|run12'.
    timed_by_tag = {}
    for r in ql_rows:
        tag = r["log_comment"].rsplit("|", 1)[-1]
        if tag.startswith("run") and tag[3:].isdigit():
            timed_by_tag.setdefault(tag, []).append(r)
    expected_tags = [f"run{i}" for i in range(runs)]
    tag_counts = {tag: len(rows) for tag, rows in timed_by_tag.items()}
    if sorted(timed_by_tag) != sorted(expected_tags) or any(n != 1 for n in tag_counts.values()):
        return {
            "status": "INVALID",
            "reason": (
                f"expected exactly one query_log row per timed run tag {expected_tags}, found {tag_counts}"
            ),
        }
    timed_rows = [timed_by_tag[tag][0] for tag in expected_tags]
    profiled_rows = [r for r in ql_rows if r["log_comment"].endswith("|profiled")]
    if len(profiled_rows) != 1:
        return {"status": "INVALID", "reason": f"expected 1 profiled query_log row, found {len(profiled_rows)}"}

    durations = [int(r["query_duration_ms"]) for r in timed_rows]
    memories = [int(r["memory_usage"]) for r in timed_rows]
    events_per_run = [_events_from_row(r) for r in timed_rows]
    # Per-run JIT compilation cost: should be zero in every timed run under
    # the 3-warmup protocol; recorded (not asserted) so `report` can flag
    # any residually contaminated cell instead of hiding it.
    jit_us_per_run = [
        int((r.get("ProfileEvents", {}) or {}).get("CompileExpressionsMicroseconds", 0))
        for r in timed_rows
    ]
    median_duration = statistics.median(durations)
    median_memory = statistics.median(memories)
    stdev_duration = statistics.pstdev(durations) if len(durations) > 1 else 0.0

    path_event = PATH_ASSERTION_EVENT[algorithm]
    path_ok = all(events[path_event] > 0 for events in events_per_run)

    summed_events: dict[str, list[int]] = {name: [e[name] for e in events_per_run] for name in events_per_run[0]}
    median_events = {name: statistics.median(vals) for name, vals in summed_events.items()}

    return {
        "status": "OK",
        "durations_ms": durations,
        "median_duration_ms": median_duration,
        "stdev_duration_ms": stdev_duration,
        "memories_bytes": memories,
        "median_memory_bytes": median_memory,
        "median_events": median_events,
        "path_assertion_ok": path_ok,
        "checksum": checksum,
        "expected_rows": expected_rows,
        "profiled_query_id": profiled_rows[0]["query_id"],
        "runs": runs,
        "warmups": WARMUP_RUNS,
        "jit_us_per_run": jit_us_per_run,
        "jit_compiled_timed_runs": sum(1 for v in jit_us_per_run if v > 0),
    }


def prepare_cell_tables(remote: RemoteArgs, cell: dict) -> int:
    cfg = KEY_CONFIGS[cell["key"]]
    run_remote_sql(remote, "CREATE DATABASE IF NOT EXISTS bench;", timeout=30)
    null_wrap = cfg.is_nullable
    rc, _, stderr = run_remote_sql(
        remote, mem_table_create_sql(BUILD_TABLE, cfg, cell["bp"], "b_p", null_wrap=null_wrap), timeout=30
    )
    require_ok(rc, stderr, "create build table")
    rc, _, stderr = run_remote_sql(
        remote, mem_table_create_sql(PROBE_TABLE, cfg, cell["pp"], "p_p", null_wrap=null_wrap), timeout=30
    )
    require_ok(rc, stderr, "create probe table")

    build_stmts = build_fill_statements(
        cfg, cell["D"], cell["m_b"], cell["bp"], cell["nulls_pct"], cell.get("skew_s", 0)
    )
    probe_stmts = probe_fill_statements(
        cfg, cell["D"], cell["m_p"], cell["h"], cell["pp"], cfg.n_max, cell["nulls_pct"]
    )
    for stmt in build_stmts + probe_stmts:
        rc, _, stderr = run_remote_sql(remote, stmt, timeout=1800)
        require_ok(rc, stderr, "fill Memory table")

    expected = expected_output_rows(
        cell["D"], cell["m_b"], cell["m_p"], cell["h"], cell["nulls_pct"], cell.get("skew_s", 0)
    )
    return expected


def detect_remote_arch(remote: RemoteArgs) -> str:
    """The benchmark host's CPU architecture (`uname -m`, e.g. `aarch64` /
    `x86_64`). Stamped into every results row and used to keep concurrent
    fleets of different architectures separate on disk; fail-closed - a row
    with a guessed architecture would silently merge two machines' data."""
    rc, stdout, stderr = run_ssh(remote, "uname -m", timeout=15)
    arch = stdout.decode("utf-8", "replace").strip()
    if rc != 0 or not arch:
        raise RuntimeError(f"cannot detect remote architecture (rc={rc}): {stderr or 'empty uname -m output'}")
    return arch


def run_cell_command(args: argparse.Namespace) -> int:
    remote = remote_args_from_ns(args)
    if args.anchor:
        cell = make_cell(**ANCHOR, threads=args.threads, group="smoke", note="smoke")
    elif getattr(args, "cell_json", None):
        cell = json.loads(args.cell_json)
    else:
        plan = build_plan()
        matches = [c for c in plan if c["cell_id"] == args.cell_id]
        if not matches:
            print(f"ERROR: cell_id {args.cell_id!r} not found in the plan", file=sys.stderr)
            return 2
        cell = matches[0]

    # Detected once per sweep and passed down; a bare run-cell detects here.
    arch = getattr(args, "detected_arch", None) or detect_remote_arch(remote)
    local_dir = pathlib.Path(args.local_root) / "cells" / arch / cell["cell_id"]
    local_dir.mkdir(parents=True, exist_ok=True)

    print(f"Cell {cell['cell_id']}: starting server ...")
    start_server(remote)
    try:
        print(f"Cell {cell['cell_id']}: filling tables ...")
        t0 = time.monotonic()
        expected_rows = prepare_cell_tables(remote, cell)
        fill_seconds = time.monotonic() - t0
        print(f"Cell {cell['cell_id']}: filled in {fill_seconds:.1f}s, expected_rows={expected_rows}")

        run_nonce = f"{int(time.time())}_{os.getpid()}"
        results = {}
        for algorithm in ALGORITHMS:
            print(f"Cell {cell['cell_id']}: running {algorithm} ...")
            t0 = time.monotonic()
            result = run_one_algorithm(
                remote, KEY_CONFIGS[cell["key"]], algorithm, cell["threads"], cell["cell_id"], run_nonce,
                expected_rows=expected_rows, runs=int(cell.get("runs", 5)),
            )
            result["wall_seconds"] = time.monotonic() - t0
            results[algorithm] = result
            print(f"Cell {cell['cell_id']}: {algorithm} -> {result['status']}" + (f" ({result.get('reason')})" if result.get("reason") else ""))
            _export_logs_for_cell(remote, local_dir, f"{cell['cell_id']}|{run_nonce}", algorithm)

        correctness = "SKIP"
        if all(results[a]["status"] == "OK" for a in ALGORITHMS):
            checks = [results[a]["checksum"] for a in ALGORITHMS]
            correctness = "PASS" if len(set(checks)) == 1 else "FAIL_CHECKSUM_MISMATCH"

        record = {
            "cell": cell,
            "arch": arch,
            "expected_rows": expected_rows,
            "results": results,
            "correctness": correctness,
            "fill_seconds": fill_seconds,
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        (local_dir / "cell_result.json").write_text(json.dumps(record, indent=2, default=str) + "\n")

        results_path = _results_path_from_args(args)
        with results_path.open("a") as f:
            for algorithm in ALGORITHMS:
                line = {
                    "cell_id": cell["cell_id"],
                    "cell": cell,
                    "arch": arch,
                    "algorithm": algorithm,
                    "expected_rows": expected_rows,
                    "correctness": correctness,
                    **results[algorithm],
                }
                f.write(json.dumps(line, default=str) + "\n")

        print(f"Cell {cell['cell_id']}: correctness={correctness}")
        return 0 if correctness in ("PASS",) and all(results[a]["status"] == "OK" for a in ALGORITHMS) else 1
    finally:
        if not args.keep_server_running:
            print(f"Cell {cell['cell_id']}: stopping server ...")
            stop_server(remote)


def _results_path_from_args(args: argparse.Namespace) -> pathlib.Path:
    if getattr(args, "results_path", None):
        return pathlib.Path(args.results_path)
    return pathlib.Path(args.local_root) / "results.jsonl"


# --------------------------------------------------------------------------
# sweep
# --------------------------------------------------------------------------


def _load_completed_cells(results_path: pathlib.Path, arch: str | None = None) -> set[str]:
    """Cells with both algorithms recorded. Rows from a DIFFERENT architecture
    never mark a cell complete (a shared results file must not let the aarch64
    fleet's rows satisfy the x86_64 sweep's resume check); legacy rows without
    an arch stamp count for any architecture."""
    if not results_path.exists():
        return set()
    counts: dict[str, set[str]] = {}
    for line in results_path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        row_arch = row.get("arch")
        if arch is not None and row_arch is not None and row_arch != arch:
            continue
        counts.setdefault(row["cell_id"], set()).add(row["algorithm"])
    return {cid for cid, algos in counts.items() if algos == set(ALGORITHMS)}


def sweep_command(args: argparse.Namespace) -> int:
    shards = getattr(args, "shards", 1) or 1
    plan = build_plan(shards=shards)
    if getattr(args, "shard", None) is not None:
        plan = [c for c in plan if c.get("shard") == args.shard]
        if not plan:
            print(f"ERROR: no cells assigned to shard {args.shard} (of {shards})", file=sys.stderr)
            return 2
    if args.group:
        plan = [c for c in plan if args.group in c["groups"]]
    if args.cell_ids:
        wanted = set(args.cell_ids.split(","))
        unknown = wanted - {c["cell_id"] for c in plan}
        if unknown:
            print(
                f"ERROR: cell ids not in the plan (a silent skip here would lose cells): "
                f"{sorted(unknown)}",
                file=sys.stderr,
            )
            return 2
        plan = [c for c in plan if c["cell_id"] in wanted]

    # Fail fast if the remote keys_store is missing or undersized for any key
    # config this sweep needs -- hours before a cell would hit it.
    remote = remote_args_from_ns(args)
    arch = detect_remote_arch(remote)
    start_server(remote)
    needed = sorted({c["key"] for c in plan})
    for key_id in needed:
        cfg = KEY_CONFIGS[key_id]
        rows = run_remote_sql_json(
            remote,
            f"SELECT count() AS n FROM system.tables WHERE database='keys_store' AND name='{key_id.lower()}' FORMAT JSONEachRow",
        )
        if int(rows[0]["n"]) == 0:
            print(f"ERROR: keys_store.{key_id.lower()} missing; run: prepare-keys --keys {','.join(needed)}", file=sys.stderr)
            return 2
        rows = run_remote_sql_json(remote, f"SELECT count() AS n FROM {keys_store_table_name(key_id)} FORMAT JSONEachRow")
        if int(rows[0]["n"]) != 2 * cfg.n_max:
            print(
                f"ERROR: keys_store.{key_id.lower()} has {rows[0]['n']} rows, expected {2 * cfg.n_max} "
                f"(N_max changed?); re-run: prepare-keys --keys {key_id}",
                file=sys.stderr,
            )
            return 2

    results_path = _results_path_from_args(args)
    completed = _load_completed_cells(results_path, arch)
    remaining = [c for c in plan if c["cell_id"] not in completed]
    print(
        f"sweep: arch={arch}, {len(plan)} planned, "
        f"{len(completed & {c['cell_id'] for c in plan})} already complete, {len(remaining)} to run"
    )

    ns = argparse.Namespace(**vars(args))
    ns.detected_arch = arch
    for i, cell in enumerate(remaining, 1):
        print(f"\n=== sweep {i}/{len(remaining)}: {cell['cell_id']} ===")
        ns.anchor = False
        ns.cell_id = cell["cell_id"]
        ns.cell_json = json.dumps(cell)
        ns.keep_server_running = args.keep_server_running
        rc = run_cell_command(ns)
        if rc != 0:
            print(f"sweep: cell {cell['cell_id']} returned rc={rc} (recorded; continuing)")
    return 0


# --------------------------------------------------------------------------
# selftest
# --------------------------------------------------------------------------


def selftest_check_events(remote: RemoteArgs) -> bool:
    names = ", ".join(f"'{n}'" for n in ALL_MAPPING_EVENTS)
    rows = run_remote_sql_json(
        remote,
        f"SELECT name FROM system.events WHERE name IN ({names}) SETTINGS system_events_show_zero_values = 1 FORMAT JSONEachRow",
    )
    found = {r["name"] for r in rows}
    missing = [n for n in ALL_MAPPING_EVENTS if n not in found]
    print(f"check-events: {len(found)}/{len(ALL_MAPPING_EVENTS)} real counters present in system.events")
    if missing:
        print(f"check-events: MISSING (fail-closed): {missing}")
    print(
        f"check-events: {MEMORY_TRACKER_PEAK_PSEUDO_EVENT} SPECIAL-CASED "
        "(client pseudo-event, not in system.events; sourced from query_log.memory_usage)"
    )
    return not missing


def selftest_check_profiler(remote: RemoteArgs) -> bool:
    log_comment = "selftest_profiler_check"
    sql = (
        "SELECT count() FROM numbers(200000000) "
        f"SETTINGS query_profiler_real_time_period_ns = 2000000, "
        "query_profiler_cpu_time_period_ns = 2000000, "
        f"log_comment = '{log_comment}', max_threads = 4 FORMAT Null"
    )
    rc, _, stderr = run_remote_sql(remote, sql, timeout=120)
    require_ok(rc, stderr, "selftest profiler probe query")
    _flush_logs(remote)
    rows = run_remote_sql_json(
        remote,
        (
            "SELECT trace_type, count() AS n FROM system.trace_log WHERE query_id IN "
            f"(SELECT query_id FROM system.query_log WHERE log_comment = '{log_comment}') "
            "GROUP BY trace_type FORMAT JSONEachRow"
        ),
    )
    counts = {r["trace_type"]: int(r["n"]) for r in rows}
    print(f"check-profiler: trace_log rows by type: {counts}")
    ok = counts.get("Real", 0) > 0 and counts.get("CPU", 0) > 0
    if not ok:
        print("check-profiler: FAIL -- expected > 0 rows for both Real and CPU trace types")
    return ok


def selftest_check_keystore(remote: RemoteArgs, key_ids: list[str], disjoint_sample: list[str]) -> bool:
    """Row-count check runs on every requested config (cheap: one `count()`).
    The hit/miss anti-join disjointness check runs only on `disjoint_sample`
    -- checked by an anti-join count of 0
    on a sample config") -- because on the 1.024B-row/64-byte-string K7
    table a full anti-join is a multi-hundred-second, tens-of-GB hash join
    (measured: >390s, 64GB+ before being cancelled for cost/benefit), which
    is disproportionate to run on every config every time."""
    ok = True
    for key_id in key_ids:
        cfg = KEY_CONFIGS[key_id]
        table = keys_store_table_name(key_id)
        rows = run_remote_sql_json(remote, f"SELECT count() AS n FROM {table} FORMAT JSONEachRow")
        n = int(rows[0]["n"])
        expected = 2 * cfg.n_max
        status = "OK" if n == expected else "FAIL"
        if status == "FAIL":
            ok = False
        print(f"check-keystore: {key_id} rows={n} expected={expected} -> {status}")

    for key_id in disjoint_sample:
        cfg = KEY_CONFIGS[key_id]
        table = keys_store_table_name(key_id)
        key_expr = ", ".join(cfg.key_columns)
        t0 = time.monotonic()
        anti = run_remote_sql_json(
            remote,
            (
                f"SELECT count() AS n FROM (SELECT {key_expr} FROM {table} WHERE rank < {cfg.n_max}) AS hit "
                f"INNER JOIN (SELECT {key_expr} FROM {table} WHERE rank >= {cfg.n_max}) AS miss "
                f"USING ({key_expr}) SETTINGS max_threads = 96, max_memory_usage = 250000000000 "
                "FORMAT JSONEachRow"
            ),
            timeout=1200,
        )
        overlap = int(anti[0]["n"])
        elapsed = time.monotonic() - t0
        status2 = "OK (disjoint)" if overlap == 0 else f"FAIL ({overlap} overlapping keys)"
        if overlap != 0:
            ok = False
        print(f"check-keystore: {key_id} hit/miss domain overlap={overlap} -> {status2} ({elapsed:.1f}s, sampled config)")
    return ok


def selftest_check_small_cells(remote: RemoteArgs, local_root: str, wrong_formula: bool) -> bool:
    ok = True
    for key in ("K0", "K7"):
        cell = make_cell(D=1_000_000, key=key, m_b=2, m_p=1, h="0.9", bp=8, pp=8, threads=16, group="selftest")
        cfg = KEY_CONFIGS[key]
        run_remote_sql(remote, "CREATE DATABASE IF NOT EXISTS bench;", timeout=30)
        null_wrap = cfg.is_nullable
        run_remote_sql(remote, mem_table_create_sql(BUILD_TABLE, cfg, cell["bp"], "b_p", null_wrap=null_wrap), timeout=30)
        run_remote_sql(remote, mem_table_create_sql(PROBE_TABLE, cfg, cell["pp"], "p_p", null_wrap=null_wrap), timeout=30)
        for stmt in build_fill_statements(cfg, cell["D"], cell["m_b"], cell["bp"], 0):
            rc, _, stderr = run_remote_sql(remote, stmt, timeout=120)
            require_ok(rc, stderr, "selftest build fill")
        for stmt in probe_fill_statements(cfg, cell["D"], cell["m_p"], cell["h"], cell["pp"], cfg.n_max, 0):
            rc, _, stderr = run_remote_sql(remote, stmt, timeout=120)
            require_ok(rc, stderr, "selftest probe fill")

        correct_expected = expected_output_rows(cell["D"], cell["m_b"], cell["m_p"], cell["h"], 0)
        used_expected = correct_expected + (1 if wrong_formula else 0)

        checksums = {}
        row_counts = {}
        for algorithm in ALGORITHMS:
            sql = join_query_sql(cfg, algorithm, cell["threads"], profiled=False, log_comment=f"selftest_{key}_{algorithm}")
            rc, stdout, stderr = run_remote_sql(remote, sql, timeout=120)
            require_ok(rc, stderr, f"selftest join {key}/{algorithm}")
            row = json.loads(stdout.decode().strip().splitlines()[0])
            checksums[algorithm] = int(row["checksum"])
            row_counts[algorithm] = int(row["row_count"])

        same_checksum = len(set(checksums.values())) == 1
        rows_match_formula = all(rc == used_expected for rc in row_counts.values())
        passed = same_checksum and rows_match_formula
        expect_str = "wrong formula (must FAIL)" if wrong_formula else "correct formula (must PASS)"
        print(
            f"check-correctness: {key} rows={row_counts} checksums_equal={same_checksum} "
            f"expected_rows={used_expected} ({expect_str}) -> {'PASS' if passed else 'FAIL'}"
        )
        if wrong_formula:
            ok = ok and (not passed)  # must-fail check
        else:
            ok = ok and passed

        # Block-size check via blockSize() referencing a real column.
        real_col = cfg.key_columns[0]
        blocks = run_remote_sql_json(
            remote,
            f"SELECT blockSize() AS bs, count() AS n FROM {BUILD_TABLE} GROUP BY blockSize() SETTINGS max_block_size=57344 FORMAT JSONEachRow",
        )
        bad_blocks = [b for b in blocks if int(b["bs"]) != 57344]
        # allow exactly one remainder block strictly smaller than 57344
        bad_blocks = [b for b in bad_blocks if int(b["bs"]) >= 57344]
        block_ok = len(bad_blocks) == 0
        if not block_ok:
            ok = False
        print(f"check-correctness: {key} block sizes (expect 57344 + one remainder) -> {blocks} -> {'PASS' if block_ok else 'FAIL'}")

        # Occurrence-major distance check: same key must reappear exactly D
        # rows later across the m_b build passes, not adjacently. Computed
        # via per-key sorted row-number arrays rather than `lagInFrame`,
        # whose no-previous-row default is 0 (not NULL for a UInt64 column),
        # which would otherwise manufacture a spurious gap=0 at the very
        # first row (rn=0) of the whole table.
        dist_rows = run_remote_sql_json(
            remote,
            (
                "SELECT min(min_gap) AS overall_min_gap, max(min_gap) AS overall_max_gap FROM ("
                f"SELECT arrayMin(arrayPopFront(arrayDifference(arraySort(groupArray(rn))))) AS min_gap "
                # `max_threads = 1` here is load-bearing: reading the Memory
                # table with more than one thread scrambles
                # `rowNumberInAllBlocks()` relative to storage/insert order
                # across read threads (measured: multi-threaded read on a
                # correctly single-threaded-inserted table gave min/max
                # gaps of ~885K..1.2M instead of exactly D=1000000; adding
                # `max_threads=1` here alone -- with no change to the
                # insert -- reproduced exactly 1000000/1000000).
                f"FROM (SELECT {real_col}, rowNumberInAllBlocks() AS rn FROM {BUILD_TABLE} SETTINGS max_threads = 1) "
                f"GROUP BY {real_col} HAVING count() > 1"
                ") FORMAT JSONEachRow"
            ),
        )
        min_gap = dist_rows[0]["overall_min_gap"] if dist_rows else None
        max_gap = dist_rows[0]["overall_max_gap"] if dist_rows else None
        gap_ok = (min_gap is None) or (int(min_gap) == cell["D"] and int(max_gap) == cell["D"])
        if not gap_ok:
            ok = False
        print(f"check-correctness: {key} occurrence-major repeat-gap min/max={min_gap}/{max_gap} expected={cell['D']} -> {'PASS' if gap_ok else 'FAIL'}")
    return ok


def selftest_check_skew(remote: RemoteArgs, wrong_formula: bool) -> bool:
    """Skew closed-form check: a small K0 cell with `skew_s` extra copies of
    the rank-0 key must produce exactly m_p*(D*m_b + S) rows on BOTH
    algorithms with equal checksums; the wrong-formula variant (off by one)
    must FAIL, proving the check has power against a bad skew closed form."""
    cfg = KEY_CONFIGS["K0"]
    cell = make_cell(
        D=1_000_000, key="K0", m_b=2, m_p=3, h="1.0", bp=8, pp=8, threads=16,
        skew_s=500_000, group="selftest",
    )
    run_remote_sql(remote, "CREATE DATABASE IF NOT EXISTS bench;", timeout=30)
    run_remote_sql(remote, mem_table_create_sql(BUILD_TABLE, cfg, cell["bp"], "b_p", null_wrap=False), timeout=30)
    run_remote_sql(remote, mem_table_create_sql(PROBE_TABLE, cfg, cell["pp"], "p_p", null_wrap=False), timeout=30)
    for stmt in build_fill_statements(cfg, cell["D"], cell["m_b"], cell["bp"], 0, cell["skew_s"]):
        rc, _, stderr = run_remote_sql(remote, stmt, timeout=120)
        require_ok(rc, stderr, "selftest skew build fill")
    for stmt in probe_fill_statements(cfg, cell["D"], cell["m_p"], cell["h"], cell["pp"], cfg.n_max, 0):
        rc, _, stderr = run_remote_sql(remote, stmt, timeout=120)
        require_ok(rc, stderr, "selftest skew probe fill")

    correct = expected_output_rows(cell["D"], cell["m_b"], cell["m_p"], cell["h"], 0, cell["skew_s"])
    used = correct + (1 if wrong_formula else 0)
    checksums, row_counts = {}, {}
    for algorithm in ALGORITHMS:
        sql = join_query_sql(cfg, algorithm, cell["threads"], profiled=False, log_comment=f"selftest_skew_{algorithm}")
        rc, stdout, stderr = run_remote_sql(remote, sql, timeout=120)
        require_ok(rc, stderr, f"selftest skew join {algorithm}")
        row = json.loads(stdout.decode().strip().splitlines()[0])
        checksums[algorithm] = int(row["checksum"])
        row_counts[algorithm] = int(row["row_count"])
    passed = len(set(checksums.values())) == 1 and all(n == used for n in row_counts.values())
    expect_str = "wrong skew formula (must FAIL)" if wrong_formula else "skew closed form (must PASS)"
    print(f"check-skew: rows={row_counts} expected={used} ({expect_str}) -> {'PASS' if passed else 'FAIL'}")
    return (not passed) if wrong_formula else passed


def selftest_check_jit(remote: RemoteArgs) -> bool:
    """Under the {WARMUP_RUNS}-warmup protocol a small (fast) cell's timed
    runs must carry zero `CompileExpressionsMicroseconds`: exactly the
    contamination that inflated the small-D stdevs."""
    cfg = KEY_CONFIGS["K0"]
    cell = make_cell(D=2_000_000, key="K0", m_b=1, m_p=1, h="1.0", bp=8, pp=8, threads=8, group="selftest")
    run_remote_sql(remote, "CREATE DATABASE IF NOT EXISTS bench;", timeout=30)
    run_remote_sql(remote, mem_table_create_sql(BUILD_TABLE, cfg, cell["bp"], "b_p", null_wrap=False), timeout=30)
    run_remote_sql(remote, mem_table_create_sql(PROBE_TABLE, cfg, cell["pp"], "p_p", null_wrap=False), timeout=30)
    for stmt in build_fill_statements(cfg, cell["D"], cell["m_b"], cell["bp"], 0):
        rc, _, stderr = run_remote_sql(remote, stmt, timeout=120)
        require_ok(rc, stderr, "selftest jit build fill")
    for stmt in probe_fill_statements(cfg, cell["D"], cell["m_p"], cell["h"], cell["pp"], cfg.n_max, 0):
        rc, _, stderr = run_remote_sql(remote, stmt, timeout=120)
        require_ok(rc, stderr, "selftest jit probe fill")
    expected = expected_output_rows(cell["D"], cell["m_b"], cell["m_p"], cell["h"], 0)
    ok = True
    nonce = f"selftestjit_{int(time.time())}_{os.getpid()}"
    for algorithm in ALGORITHMS:
        result = run_one_algorithm(
            remote, cfg, algorithm, cell["threads"], cell["cell_id"], nonce, expected_rows=expected, runs=5
        )
        if result["status"] != "OK":
            print(f"check-jit: {algorithm} run failed: {result.get('reason')} -> FAIL")
            ok = False
            continue
        contaminated = result["jit_compiled_timed_runs"]
        status = "PASS" if contaminated == 0 else "FAIL"
        if contaminated:
            ok = False
        print(
            f"check-jit: {algorithm} timed runs with CompileExpressionsMicroseconds: "
            f"{contaminated}/5 (jit_us_per_run={result['jit_us_per_run']}) -> {status}"
        )
    return ok


def selftest_command(args: argparse.Namespace) -> int:
    remote = remote_args_from_ns(args)
    all_ok = True
    if args.check_events or args.all:
        start_server(remote)
        all_ok = selftest_check_events(remote) and all_ok
    if args.check_profiler or args.all:
        start_server(remote)
        all_ok = selftest_check_profiler(remote) and all_ok
    if args.check_keystore or args.all:
        start_server(remote)
        key_ids = args.keys.split(",") if args.keys else list(KEY_CONFIGS)
        # Default sample: K0 (numeric, cheap) and K4 (string, cheap) cover
        # both key-generation code paths without K7's ~70GB/1.024B-row
        # anti-join cost; pass --disjoint-sample explicitly to include K7.
        disjoint_sample = args.disjoint_sample.split(",") if args.disjoint_sample else ["K0", "K4"]
        all_ok = selftest_check_keystore(remote, key_ids, disjoint_sample) and all_ok
    if args.check_correctness or args.all:
        start_server(remote)
        all_ok = selftest_check_small_cells(remote, args.local_root, wrong_formula=False) and all_ok
        # deliberately-wrong-formula must-fail check, isolated so it never
        # pollutes the correctness gate above.
        all_ok = selftest_check_small_cells(remote, args.local_root, wrong_formula=True) and all_ok
    if args.check_skew or args.all:
        start_server(remote)
        all_ok = selftest_check_skew(remote, wrong_formula=False) and all_ok
        all_ok = selftest_check_skew(remote, wrong_formula=True) and all_ok
    if args.check_jit or args.all:
        start_server(remote)
        all_ok = selftest_check_jit(remote) and all_ok
    print(f"\nselftest overall: {'PASS' if all_ok else 'FAIL'}")
    return 0 if all_ok else 1


# --------------------------------------------------------------------------
# report
# --------------------------------------------------------------------------


def _noise_band_tie(median_a: float, median_b: float, stdev_a: float, stdev_b: float) -> bool:
    if median_a == 0 and median_b == 0:
        return True
    diff = abs(median_a - median_b)
    band = max(0.05 * max(median_a, median_b), max(stdev_a, stdev_b))
    return diff <= band


def load_results(results_spec: pathlib.Path | str, arch: str | None = None) -> dict[str, dict[str, dict]]:
    """Load one results file, or several comma-separated ones merged (the
    multi-instance mode writes one file per shard). Later files override
    earlier ones on a (cell_id, algorithm) collision -- collisions only
    happen when a cell was re-run, and the last line is the freshest,
    matching single-file append semantics. With `arch`, rows of OTHER
    architectures are dropped (legacy rows without an arch stamp always
    pass); without it, everything merges - callers that render one table
    must first prove the results are single-architecture (`result_arches`)."""
    by_cell: dict[str, dict[str, dict]] = {}
    for part in str(results_spec).split(","):
        path = pathlib.Path(part.strip())
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            row_arch = row.get("arch")
            if arch is not None and row_arch is not None and row_arch != arch:
                continue
            by_cell.setdefault(row["cell_id"], {})[row["algorithm"]] = row
    return by_cell


def result_arches(results_spec: pathlib.Path | str) -> set[str | None]:
    """The distinct architecture stamps present in the results files
    (`None` = legacy rows recorded before the stamp existed)."""
    arches: set[str | None] = set()
    for part in str(results_spec).split(","):
        path = pathlib.Path(part.strip())
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            arches.add(json.loads(line).get("arch"))
    return arches


def report_coverage(args: argparse.Namespace) -> int:
    """Coverage per architecture: each architecture found in the results (or
    the one selected with --arch) is checked independently against the plan."""
    plan = build_plan()
    if args.group:
        plan = [c for c in plan if args.group in c["groups"]]

    if getattr(args, "arch", None):
        arches: list[str | None] = [args.arch]
    else:
        found = result_arches(args.results)
        arches = sorted(found, key=lambda a: a or "") or [None]

    rc = 0
    for arch in arches:
        by_cell = load_results(args.results, arch=arch)
        missing = []
        invalid = []
        ok_missing_exports = []
        for cell in plan:
            cid = cell["cell_id"]
            algos = by_cell.get(cid, {})
            if set(algos) != set(ALGORITHMS):
                missing.append(cid)
                continue
            statuses = {a: algos[a]["status"] for a in ALGORITHMS}
            if any(s != "OK" for s in statuses.values()):
                invalid.append((cid, statuses))
                continue
            cells_root = pathlib.Path(args.local_root) / "cells"
            candidates = [cells_root / arch / cid] if arch else []
            candidates.append(cells_root / cid)  # legacy pre-arch layout
            needed = [f"query_log.{a}.jsonl" for a in ALGORITHMS]
            if not any(d.exists() and all((d / f).exists() for f in needed) for d in candidates):
                ok_missing_exports.append(cid)
        label = f"arch={arch or 'unstamped'} " if len(arches) > 1 or arch else ""
        print(
            f"coverage: {label}planned={len(plan)} missing={len(missing)} "
            f"invalid={len(invalid)} ok_missing_exports={len(ok_missing_exports)}"
        )
        for cid in missing:
            print(f"  MISSING: {cid}")
        for cid, statuses in invalid:
            print(f"  INVALID: {cid} {statuses}")
        for cid in ok_missing_exports:
            print(f"  OK_BUT_MISSING_EXPORTS: {cid}")
        if missing or ok_missing_exports:
            rc = 1
    return rc


def _format_ms(v: float) -> str:
    return f"{v:.2f}"


def _format_mb(v: float) -> str:
    return f"{v / (1024 * 1024):.1f}"


def _quantile(sorted_vals: list[float], q: float) -> float:
    """Nearest-rank-with-interpolation quantile on an already-sorted list."""
    if not sorted_vals:
        raise ValueError("no values")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = q * (len(sorted_vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def _phase_attribution(ph: dict, pl: dict) -> str:
    """One-line phase comparison for the regret table: summed-thread-time
    ratios (partitioned / parallel) for build total and probe lookup."""
    ev_ph = ph.get("median_events", {}) or {}
    ev_pl = pl.get("median_events", {}) or {}
    build_ph = float(ev_ph.get("PartitionedHashJoinBuildMicroseconds", 0))
    build_pl = float(ev_pl.get("ConcurrentHashJoinBuildMicroseconds", 0))
    lookup_ph = float(ev_ph.get("PartitionedHashJoinProbeLookupMicroseconds", 0))
    lookup_pl = float(
        ev_pl.get("ConcurrentHashJoinProbeDispatchMicroseconds", 0)
        + ev_pl.get("ConcurrentHashJoinProbeLookupMicroseconds", 0)
    )
    build = f"{build_ph / build_pl:.2f}" if build_pl else "n/a"
    lookup = f"{lookup_ph / lookup_pl:.2f}" if lookup_pl else "n/a"
    return f"build {build}x, probe-lookup {lookup}x (ph/pl thread-time)"


def generate_report_markdown(
    results_path: pathlib.Path | str,
    local_root: str,
    *,
    arch: str | None = None,
    baseline_path: str | None = None,
) -> tuple[str, dict]:
    """One architecture per report: without --arch the results must be
    single-architecture (fail-closed otherwise - averaging two machines'
    medians into one table would be silently wrong)."""
    if arch is None:
        stamped = {a for a in result_arches(results_path) if a is not None}
        if len(stamped) > 1:
            raise RuntimeError(
                f"results contain multiple architectures {sorted(stamped)}; pass --arch to render one report per architecture"
            )
        arch = next(iter(stamped), None)
    by_cell = load_results(results_path, arch=arch)
    plan_by_id = {c["cell_id"]: c for c in build_plan()}

    lines: list[str] = []
    wins = ties = losses = invalid = 0
    losing_cells: list[tuple[str, str]] = []
    tie_cells: list[str] = []
    invalid_cells: list[str] = []
    per_thread: dict[int, list[dict]] = {}

    for cell_id, algos in sorted(by_cell.items()):
        cell = algos[ALGORITHMS[0]].get("cell") or plan_by_id.get(cell_id, {})
        threads = cell.get("threads", 0)
        row = {"cell_id": cell_id, "cell": cell}
        statuses = {a: algos.get(a, {}).get("status") for a in ALGORITHMS}
        if any(statuses.get(a) != "OK" for a in ALGORITHMS):
            invalid += 1
            invalid_cells.append(cell_id)
            row["verdict"] = "INVALID"
            per_thread.setdefault(threads, []).append(row)
            continue
        ph = algos["partitioned_hash"]
        pl = algos["parallel_hash"]
        m_ph, m_pl = ph["median_duration_ms"], pl["median_duration_ms"]
        s_ph, s_pl = ph.get("stdev_duration_ms", 0.0), pl.get("stdev_duration_ms", 0.0)
        tie = _noise_band_tie(m_ph, m_pl, s_ph, s_pl)
        if tie:
            verdict = "tie"
            ties += 1
            tie_cells.append(cell_id)
        elif m_ph < m_pl:
            verdict = "partitioned_hash"
            wins += 1
        else:
            verdict = "parallel_hash"
            losses += 1
            losing_cells.append((cell_id, verdict))
        row["verdict"] = verdict
        row["partitioned_ms"] = m_ph
        row["parallel_ms"] = m_pl
        row["speedup"] = (m_pl / m_ph) if m_ph else None
        row["mem_ratio"] = (ph["median_memory_bytes"] / pl["median_memory_bytes"]) if pl["median_memory_bytes"] else None
        row["jit_flag"] = (ph.get("jit_compiled_timed_runs", 0) or 0) + (pl.get("jit_compiled_timed_runs", 0) or 0)
        row["ph"] = ph
        row["pl"] = pl
        per_thread.setdefault(threads, []).append(row)

    total_scored = wins + ties + losses
    claim = (
        f"partitioned_hash wins {wins}/{total_scored} scored cells "
        f"(ties={ties}, losses={losses}, invalid={invalid}, total_cells_with_results={len(by_cell)})"
    )
    if losses == 0 and ties == 0 and invalid == 0:
        claim = f"partitioned_hash wins {wins}/{total_scored} cells (no ties, no losses, no INVALID cells)"
    elif losses > 0 or ties > 0:
        claim = (
            "partitioned_hash does NOT win all cells: "
            f"wins={wins} ties={ties} losses={losses} invalid={invalid} out of {len(by_cell)} cells with recorded results "
            f"(losing cells: {[c for c, _ in losing_cells]}; tie cells: {tie_cells})"
        )

    lines.append("<!-- generated by `report`; do not hand-edit -->")
    lines.append(f"Architecture: `{arch or 'unstamped (legacy results)'}`\n")
    lines.append(f"## Claim line\n\n**{claim}**\n")
    lines.append(
        "Noise band: a cell's medians are classified `tie` when "
        "`abs(median_partitioned - median_parallel) <= max(5% of the larger median, max(cross-run stdev of either algorithm))`.\n"
    )

    scored_rows = [r for t in per_thread.values() for r in t if r["verdict"] != "INVALID"]

    # Regret table: every cell where parallel_hash's median is lower --
    # regardless of tie classification -- with magnitude and a phase hint.
    # This is the table a replacement decision hinges on.
    regret_rows = sorted(
        (r for r in scored_rows if r["parallel_ms"] < r["partitioned_ms"]),
        key=lambda r: r["speedup"],
    )
    lines.append("\n## Regret table (all cells where parallel_hash's median is lower)\n")
    if not regret_rows:
        lines.append("None: partitioned_hash's median is lower or equal in every scored cell.")
    else:
        lines.append("| cell_id | verdict | partitioned_ms | parallel_ms | regret | phase attribution |")
        lines.append("|---|---|---|---|---|---|")
        for r in regret_rows:
            regret_pct = (r["partitioned_ms"] / r["parallel_ms"] - 1.0) * 100.0
            lines.append(
                f"| {r['cell_id']} | {r['verdict']} | {_format_ms(r['partitioned_ms'])} | "
                f"{_format_ms(r['parallel_ms'])} | +{regret_pct:.1f}% | {_phase_attribution(r['ph'], r['pl'])} |"
            )

    # Speedup distribution.
    speedups = sorted(r["speedup"] for r in scored_rows if r["speedup"])
    if speedups:
        lines.append("\n## Speedup distribution (parallel_ms / partitioned_ms, scored cells)\n")
        lines.append("| min | p10 | p25 | median | p75 | p90 | max | n |")
        lines.append("|---|---|---|---|---|---|---|---|")
        lines.append(
            "| " + " | ".join(
                f"{_quantile(speedups, q):.3f}" for q in (0.0, 0.10, 0.25, 0.50, 0.75, 0.90, 1.0)
            ) + f" | {len(speedups)} |"
        )
        mem_ratios = sorted(r["mem_ratio"] for r in scored_rows if r["mem_ratio"])
        lines.append("\n| mem-ratio min | p50 | p90 | max |")
        lines.append("|---|---|---|---|")
        lines.append(
            "| " + " | ".join(f"{_quantile(mem_ratios, q):.3f}" for q in (0.0, 0.50, 0.90, 1.0)) + " |"
        )

    # JIT contamination flags (should be empty under the 3-warmup protocol).
    jit_flagged = [r for r in scored_rows if r.get("jit_flag")]
    lines.append("\n## JIT-contaminated timed runs (flag; expected empty)\n")
    if jit_flagged:
        for r in jit_flagged:
            lines.append(
                f"- {r['cell_id']}: {r['jit_flag']} timed run(s) carried "
                "`CompileExpressionsMicroseconds` despite the warmups -- treat the cell's stdev with suspicion."
            )
    else:
        lines.append("None.")

    # Sentinel drift vs a baseline results file, when given.
    if baseline_path:
        base_by_cell = load_results(baseline_path, arch=arch)
        shared = sorted(set(by_cell) & set(base_by_cell))
        lines.append(f"\n## Sentinel drift vs baseline ({baseline_path})\n")
        if not shared:
            lines.append("No overlapping cell_ids with the baseline.")
        else:
            lines.append("| cell_id | algo | baseline_ms | current_ms | drift | within band |")
            lines.append("|---|---|---|---|---|---|")
            for cid in shared:
                for algo in ALGORITHMS:
                    cur = by_cell[cid].get(algo)
                    base = base_by_cell[cid].get(algo)
                    if not cur or not base or cur.get("status") != "OK" or base.get("status") != "OK":
                        continue
                    m_cur, m_base = cur["median_duration_ms"], base["median_duration_ms"]
                    band = max(
                        0.05 * max(m_cur, m_base),
                        max(cur.get("stdev_duration_ms", 0.0), base.get("stdev_duration_ms", 0.0)),
                    )
                    drift_pct = ((m_cur - m_base) / m_base * 100.0) if m_base else 0.0
                    ok = abs(m_cur - m_base) <= band
                    lines.append(
                        f"| {cid} | {algo} | {_format_ms(m_base)} | {_format_ms(m_cur)} | "
                        f"{drift_pct:+.1f}% | {'yes' if ok else 'NO'} |"
                    )

    for threads in sorted(per_thread):
        lines.append(f"\n### T={threads}\n")
        lines.append(
            "| cell_id | D | key | m_b | m_p | h | bp | pp | partitioned_ms | parallel_ms | speedup | mem_ratio(ph/par) | verdict |"
        )
        lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
        for row in per_thread[threads]:
            c = row["cell"]
            if row["verdict"] == "INVALID":
                lines.append(
                    f"| {row['cell_id']} | {c.get('D')} | {c.get('key')} | {c.get('m_b')} | {c.get('m_p')} | "
                    f"{c.get('h')} | {c.get('bp')} | {c.get('pp')} | - | - | - | - | INVALID |"
                )
                continue
            lines.append(
                f"| {row['cell_id']} | {c.get('D')} | {c.get('key')} | {c.get('m_b')} | {c.get('m_p')} | "
                f"{c.get('h')} | {c.get('bp')} | {c.get('pp')} | {_format_ms(row['partitioned_ms'])} | "
                f"{_format_ms(row['parallel_ms'])} | {row['speedup']:.3f}x | {row['mem_ratio']:.3f} | {row['verdict']} |"
            )

    summary = {
        "wins": wins,
        "ties": ties,
        "losses": losses,
        "invalid": invalid,
        "total": len(by_cell),
        "losing_cells": losing_cells,
        "tie_cells": tie_cells,
        "invalid_cells": invalid_cells,
        "claim": claim,
    }
    return "\n".join(lines), summary


def report_command(args: argparse.Namespace) -> int:
    if args.coverage:
        return report_coverage(args)
    markdown, summary = generate_report_markdown(
        args.results,
        args.local_root,
        arch=getattr(args, "arch", None),
        baseline_path=getattr(args, "baseline", None),
    )
    print(markdown)
    print("\n---\nSummary:", json.dumps(summary, default=str), file=sys.stderr)
    if args.out:
        pathlib.Path(args.out).write_text(markdown + "\n")
    return 0


# --------------------------------------------------------------------------
# argparse wiring
# --------------------------------------------------------------------------


PHASE2_GROUPS = ("A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "R")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_plan = sub.add_parser("plan", help="emit the full cell plan as JSON")
    p_plan.add_argument("--out", help="write plan JSON to this path instead of stdout")
    p_plan.add_argument("--shards", type=int, default=4, help="instance count for shard assignment (default: 4)")
    p_plan.add_argument("--shard-summary", action="store_true", help="print per-shard cell counts, est cost, and required key configs")
    p_plan.set_defaults(handler=plan_command)

    p_prep = sub.add_parser("prepare-keys", help="create/verify the persistent keys_store tables")
    add_remote_args(p_prep)
    p_prep.add_argument("--local-root", default=DEFAULT_LOCAL_ROOT)
    p_prep.add_argument("--keys", help="comma-separated key ids (default: all K0..K9)")
    p_prep.set_defaults(handler=prepare_keys_command)

    p_run = sub.add_parser("run-cell", help="run one cell (both algorithms) end-to-end")
    add_remote_args(p_run)
    p_run.add_argument("--local-root", default=DEFAULT_LOCAL_ROOT)
    p_run.add_argument("--cell-id")
    p_run.add_argument("--cell-json", help=argparse.SUPPRESS)  # internal: sweep passes the full cell dict
    p_run.add_argument("--anchor", action="store_true", help="run the anchor cell instead of --cell-id")
    p_run.add_argument("--threads", type=int, default=96)
    p_run.add_argument("--results-path", help="append results here (default: <local-root>/results.jsonl)")
    p_run.add_argument("--keep-server-running", action="store_true")
    p_run.set_defaults(handler=run_cell_command)

    p_sweep = sub.add_parser("sweep", help="run every not-yet-complete planned cell, resumably")
    add_remote_args(p_sweep)
    p_sweep.add_argument("--local-root", default=DEFAULT_LOCAL_ROOT)
    p_sweep.add_argument("--shards", type=int, default=4, help="total shard count the plan is split into")
    p_sweep.add_argument("--shard", type=int, help="run only this shard's cells (multi-instance mode)")
    p_sweep.add_argument("--group", choices=("backbone", "ofat", "interaction") + PHASE2_GROUPS)
    p_sweep.add_argument("--cell-ids", help="comma-separated explicit cell_id allowlist")
    p_sweep.add_argument("--results-path", help="append results here (default: <local-root>/results.jsonl); use one file per shard")
    p_sweep.add_argument("--keep-server-running", action="store_true")
    p_sweep.set_defaults(handler=sweep_command)

    p_self = sub.add_parser("selftest", help="run correctness/profiler/event-existence self-checks")
    add_remote_args(p_self)
    p_self.add_argument("--local-root", default=DEFAULT_LOCAL_ROOT)
    p_self.add_argument("--check-events", action="store_true")
    p_self.add_argument("--check-profiler", action="store_true")
    p_self.add_argument("--check-keystore", action="store_true")
    p_self.add_argument("--check-correctness", action="store_true")
    p_self.add_argument("--check-skew", action="store_true")
    p_self.add_argument("--check-jit", action="store_true")
    p_self.add_argument("--all", action="store_true")
    p_self.add_argument("--keys", help="comma-separated key ids for --check-keystore (default: all)")
    p_self.add_argument(
        "--disjoint-sample",
        help="comma-separated key ids to run the (expensive) hit/miss anti-join disjointness check on (default: K0,K7)",
    )
    p_self.set_defaults(handler=selftest_command)

    p_report = sub.add_parser("report", help="regenerate REPORT.md (or check coverage) from results.jsonl")
    p_report.add_argument(
        "--results",
        default=f"{DEFAULT_LOCAL_ROOT}/results.jsonl",
        help="results file(s); comma-separated to merge per-shard files",
    )
    p_report.add_argument("--local-root", default=DEFAULT_LOCAL_ROOT)
    p_report.add_argument("--arch", help="architecture to report on (e.g. aarch64, x86_64); required when the results contain more than one")
    p_report.add_argument("--out", help="write markdown to this path")
    p_report.add_argument("--coverage", action="store_true", help="check plan coverage instead of rendering tables")
    p_report.add_argument(
        "--group",
        choices=("backbone", "ofat", "interaction") + PHASE2_GROUPS,
        help="restrict --coverage to one group",
    )
    p_report.add_argument(
        "--baseline",
        help="a baseline results.jsonl (e.g. a previous run of the same architecture); adds a sentinel-drift section for cells present in both",
    )
    p_report.set_defaults(handler=report_command)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
