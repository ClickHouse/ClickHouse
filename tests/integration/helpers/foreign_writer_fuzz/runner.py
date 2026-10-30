"""Execute histories, check every step, report and shrink failures."""

import json
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

from .context import ClickHouseQueryError
from .model import TableModel
from .operations import (
    REGISTRY,
    describe_op,
    dumps_history,
    generate_history,
    validate_op,
)
from .oracle import FuzzFailure, Oracle, SchemaMismatch, canonical_rows, content_hash
from .writers import WRITERS, ForeignReadError

log = logging.getLogger("foreign_writer_fuzz")

SERVER_BUG_MARKERS = ("LOGICAL_ERROR", "Logical error", "Received signal", "Sanitizer")


@dataclass
class RunResult:
    history: dict
    executed: List[int] = field(default_factory=list)
    rejected: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    checked_steps: int = 0
    queries: int = 0
    truncated: bool = False
    failure: Optional[FuzzFailure] = None
    failed_op_index: Optional[int] = None
    seconds: float = 0.0

    def summary(self):
        h = self.history
        return (
            f"{h['writer']} seed {h['seed']}: {len(self.executed)} ops executed, {len(self.rejected)} rejected by the writer, "
            f"{len(self.skipped)} skipped, {self.checked_steps} steps checked with {self.queries} ClickHouse queries "
            f"in {self.seconds:.0f} s{' (time budget hit)' if self.truncated else ''}"
        )


def writer_caps(writer_name, gates):
    cls = WRITERS[writer_name]
    return cls.adjust_caps(cls.caps, gates)


def make_history(writer_name, seed, num_ops, gates):
    history = generate_history(
        seed, writer_name, writer_caps(writer_name, gates), gates, num_ops
    )
    for i, op in enumerate(history["ops"]):
        op["i"] = i
    return history


def _oracle_rng(history, op):
    return random.Random(
        f"{history['writer']}:{history['seed']}:oracle:{op.get('i', 0)}"
    )


def _is_server_bug(text):
    return any(marker in text for marker in SERVER_BUG_MARKERS)


def _read_expected(writer, model):
    """Canonical rows from the writer's own reader(s). With several foreign readers (ClickHouse is the
    writer) they must agree with each other; the first one is the reference."""
    try:
        result = writer.read()
    except ForeignReadError as e:
        raise FuzzFailure(
            "foreign_read", str(e), {"source": "foreign reader"}
        ) from None
    if not isinstance(result, dict):
        return canonical_rows(result, model.columns)
    reference_name, reference = None, None
    for name, table in result.items():
        try:
            rows = canonical_rows(table, model.columns)
        except SchemaMismatch as e:
            raise FuzzFailure(
                "foreign_read",
                f"{name} returned an unexpected schema: {e}",
                {"source": name},
            ) from None
        if reference is None:
            reference_name, reference = name, rows
        elif sorted(map(repr, rows)) != sorted(map(repr, reference)):
            from .oracle import diff_rows

            raise FuzzFailure(
                "foreign_disagree",
                f"foreign readers {reference_name} and {name} disagree",
                {"source": name, "diff": diff_rows(reference, rows)},
            )
    return reference


def run_history(ctx, history, deadline=None, oracle=None, check_every_step=True):
    """Execute one history and check it. Never raises for ClickHouse bugs: they are returned in
    `RunResult.failure`. Harness errors (a writer that cannot read its own table) do raise.
    """
    started = time.monotonic()
    result = RunResult(history=history)
    writer_cls = WRITERS[history["writer"]]
    model = TableModel.from_json(history["table"])
    table_name = f"fwf_{history['writer']}_{history['seed']}_{uuid.uuid4().hex[:8]}"
    writer = writer_cls(ctx, table_name, model)
    oracle = oracle or Oracle(ctx)
    queries_before = oracle.queries
    log_line = ctx.server_log_lines()
    snapshots = {}  # ref -> (columns, rows)
    log.info(
        "History %s seed %s: table %s, %s",
        history["writer"],
        history["seed"],
        table_name,
        model.describe(),
    )
    try:
        writer.create()
        for index, op in enumerate(history["ops"]):
            if deadline is not None and time.monotonic() > deadline:
                result.truncated = True
                break
            label = f"op {index} {describe_op(op)}"
            result.failed_op_index = index
            reason = validate_op(model, op)
            if reason:
                result.skipped.append(f"{label}: {reason}")
                continue
            spec = REGISTRY[op["kind"]]
            payload = spec.prepare(model, op)
            try:
                writer.execute(op, payload)
            except ClickHouseQueryError as e:
                if _is_server_bug(str(e)):
                    raise FuzzFailure(
                        "server_error",
                        f"ClickHouse failed with a server bug executing {label}: {str(e)[:3000]}",
                        {"source": "writer"},
                    )
                result.rejected.append(f"{label}: {str(e).splitlines()[0][:300]}")
                log.info("Rejected %s: %s", label, str(e)[:500])
                continue
            except Exception as e:
                # A foreign writer refusing an operation (`OpRejected` or any other error) is not a ClickHouse bug.
                if isinstance(e, FuzzFailure):
                    raise
                result.rejected.append(
                    f"{label}: {type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"
                )
                log.info("Rejected %s: %s: %s", label, type(e).__name__, str(e)[:1000])
                continue
            spec.apply_schema(model, op)
            result.executed.append(index)
            log.info("Executed %s", label)
            if not writer.readable() or not writer.checkable():
                continue
            if not check_every_step and index != len(history["ops"]) - 1:
                continue
            expected = _read_expected(writer, model)
            rng = _oracle_rng(history, op)
            oracle.check(writer, model.columns, expected, rng, label)
            ref = writer.snapshot_ref()
            if ref is not None and ref not in snapshots:
                # Keep the schema the snapshot was committed with: metadata-only changes (an Iceberg
                # rename) keep the current snapshot, and time travel reads with the snapshot's schema.
                snapshots[ref] = ([list(c) for c in model.columns], expected)
            _check_time_travel(writer, oracle, snapshots, ref, rng, label)
            _check_server_log(ctx, log_line, label)
            result.checked_steps += 1
            log.info(
                "Checked %s: %s rows, content hash %s",
                label,
                len(expected),
                content_hash(expected),
            )
        result.failed_op_index = None
    except FuzzFailure as failure:
        result.failure = failure
        if not ctx.server_alive():
            failure.details["server"] = "the server is not responding after the failure"
        try:
            errors = ctx.server_errors_since(log_line)
            if errors:
                failure.details["server_log"] = errors
        except Exception:
            pass
    finally:
        result.queries = oracle.queries - queries_before
        result.seconds = time.monotonic() - started
        try:
            writer.cleanup()
        except Exception as e:
            log.warning("Cleanup of %s failed: %s", table_name, e)
    log.info(result.summary())
    return result


def _check_time_travel(writer, oracle, snapshots, current_ref, rng, label):
    live = [r for r in writer.live_refs(list(snapshots)) if r != current_ref]
    for dead in set(snapshots) - set(live) - {current_ref}:
        del snapshots[dead]
    if not live:
        return
    ref = rng.choice(sorted(live))
    columns, rows = snapshots[ref]
    settings = writer.time_travel_settings(ref)
    if settings:
        oracle.check(
            writer,
            columns,
            rows,
            rng,
            f"{label} (time travel to {ref})",
            deep=False,
            extra_settings=settings,
        )


def _check_server_log(ctx, line, label):
    errors = ctx.server_errors_since(line)
    if errors:
        raise FuzzFailure(
            "server_log",
            f"the server log has errors after {label}",
            {"source": "server log", "server_log": errors},
        )
    if not ctx.server_alive():
        raise FuzzFailure(
            "server_dead",
            f"the server stopped responding after {label}",
            {"source": "server"},
        )


# --------------------------------------------------------------------- shrinking
def truncate_history(history, last_index):
    h = json.loads(json.dumps(history))
    h["ops"] = h["ops"][: last_index + 1]
    return h


def shrink(ctx, history, failure, deadline):
    """Greedy delta debugging over the operations: drop chunks of operations while the history still
    fails with the same signature. Returns the smallest failing history found before `deadline`.
    """
    signature = failure.signature()
    best = history
    chunk = max(1, len(best["ops"]) // 2)
    attempts = 0
    while chunk >= 1 and time.monotonic() < deadline:
        progress = False
        start = 0
        while start < len(best["ops"]) and time.monotonic() < deadline:
            candidate = json.loads(json.dumps(best))
            candidate["ops"] = (
                candidate["ops"][:start] + candidate["ops"][start + chunk :]
            )
            if not candidate["ops"]:
                start += chunk
                continue
            attempts += 1
            # Checking only the last step is much faster; a time-travel failure needs the earlier snapshots.
            r = run_history(
                ctx,
                candidate,
                deadline=deadline,
                check_every_step=failure.kind == "time_travel",
            )
            if r.failure is not None and r.failure.signature() == signature:
                best = (
                    truncate_history(candidate, r.failed_op_index)
                    if r.failed_op_index is not None
                    else candidate
                )
                progress = True
                log.info("Shrunk to %s ops", len(best["ops"]))
            else:
                start += chunk
        if not progress:
            chunk //= 2
    log.info(
        "Shrinking finished after %s attempts: %s -> %s ops",
        attempts,
        len(history["ops"]),
        len(best["ops"]),
    )
    return best


# --------------------------------------------------------------------- reporting
def failure_report(result, report_dir=None, shrunk=None):
    failure = result.failure
    history = (
        truncate_history(result.history, result.failed_op_index)
        if result.failed_op_index is not None
        else result.history
    )
    minimal = shrunk or history
    path = None
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
        path = os.path.join(
            report_dir,
            f"foreign_writer_fuzz_{history['writer']}_{history['seed']}.json",
        )
        with open(path, "w") as f:
            f.write(dumps_history(minimal))
    lines = [
        f"Foreign writer fuzz failure: writer {history['writer']}, seed {history['seed']}, check {failure.kind}",
        failure.message,
        "",
        "Details:",
        json.dumps(failure.details, indent=2, default=str, ensure_ascii=False)[:8000],
        "",
        f"Initial table: {TableModel.from_json(minimal['table']).describe()}",
        "Operations of the minimal failing history:",
    ]
    lines += [f"  {i}: {describe_op(op)}" for i, op in enumerate(minimal["ops"])]
    lines += [
        "",
        f"Run so far: {result.summary()}",
        f"Rejected by the writer: {result.rejected[:10]}",
        "",
        "Replay (from the repository root):",
        f"  python -m ci.praktika run integration --test test_datalake_foreign_writer_fuzz/test.py::test_replay --param FOREIGN_WRITER_FUZZ_REPLAY={path or '<history.json>'}",
        f"  or regenerate it: --param FOREIGN_WRITER_FUZZ_WRITERS={history['writer']},FOREIGN_WRITER_FUZZ_SEED_LIST={history['seed']}",
        "History JSON:",
        dumps_history(minimal),
    ]
    return "\n".join(lines)
