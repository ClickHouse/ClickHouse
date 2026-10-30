"""Differential fuzzing of data lake tables written by other tools (and by ClickHouse for other tools).

For every writer (Spark Iceberg, PyIceberg through a REST catalog, Spark Delta, delta-rs, Spark Hudi,
Spark Paimon, and ClickHouse writing Iceberg and Delta), seeded random histories of appends,
overwrites, row-level deletes, updates, merges, schema and partition evolution, compaction and
snapshot expiry are executed by the writer. After each step ClickHouse must return exactly what the
writer's own reader returns: content, counts, aggregates, filtered reads under random predicates
and read settings, and time travel. See `helpers/foreign_writer_fuzz/README.md`.

A PR run uses a few fixed seeds per writer and a short time budget; set `FOREIGN_WRITER_FUZZ_*`
(see `helpers/foreign_writer_fuzz/budget.py`) for a long fuzzing run or to replay a failure.
"""

import json
import logging
import time

import pytest

from helpers.foreign_writer_fuzz import (
    WRITERS,
    failure_report,
    load_budget,
    make_history,
    run_history,
    shrink,
)

BUDGET = load_budget()
WRITER_NAMES = [w for w in WRITERS if BUDGET.writers is None or w in BUDGET.writers]

log = logging.getLogger("foreign_writer_fuzz")


@pytest.mark.timeout(BUDGET.pytest_timeout)
@pytest.mark.parametrize("writer", WRITER_NAMES)
def test_foreign_writer_fuzz(fuzz_context, writer):
    ctx = fuzz_context
    log.info(
        "Writer %s, seeds %s, %s ops each, budget %s s, %s",
        writer,
        BUDGET.seeds,
        BUDGET.ops,
        BUDGET.time_budget,
        ctx.gates.describe(),
    )
    deadline = time.monotonic() + BUDGET.time_budget
    reports, signatures, results = [], set(), []
    for seed in BUDGET.seeds:
        if time.monotonic() > deadline:
            log.info("Time budget exhausted before seed %s", seed)
            break
        history = make_history(writer, seed, BUDGET.ops, ctx.gates)
        result = run_history(ctx, history, deadline=deadline)
        results.append(result)
        if result.failure is None:
            continue
        if result.failure.signature() in signatures:
            continue
        signatures.add(result.failure.signature())
        shrunk = None
        if BUDGET.shrink_seconds > 0:
            failing = json.loads(json.dumps(history))
            failing["ops"] = failing["ops"][
                : (
                    result.failed_op_index
                    if result.failed_op_index is not None
                    else len(failing["ops"]) - 1
                )
                + 1
            ]
            shrunk = shrink(
                ctx, failing, result.failure, time.monotonic() + BUDGET.shrink_seconds
            )
        report = failure_report(result, ctx.cluster.instances_dir, shrunk)
        log.error(report)
        reports.append(report)
    log.info("Writer %s summary:\n%s", writer, "\n".join(r.summary() for r in results))
    assert results, "the time budget did not allow a single history"
    assert (
        any(r.checked_steps for r in results) or reports
    ), f"no step of any history was checked: {[r.rejected for r in results]}"
    if reports:
        pytest.fail("\n\n".join(reports), pytrace=False)


@pytest.mark.skipif(
    not BUDGET.replay, reason="set FOREIGN_WRITER_FUZZ_REPLAY to a history JSON file"
)
def test_replay(fuzz_context):
    with open(BUDGET.replay) as f:
        history = json.load(f)
    result = run_history(fuzz_context, history)
    log.info(result.summary())
    if result.failure is not None:
        pytest.fail(
            failure_report(result, fuzz_context.cluster.instances_dir), pytrace=False
        )
