"""
Tests for the integration job's nested-container memory budget.

The job runs a nested Docker daemon inside a container with an outer `--memory` limit, and
every test container is created through that daemon. `docker_in_docker.sh` splits the limit
into three capped cgroup leaves, whose caps may sum above it, and `INTEGRATION_NESTED_BUDGET`
is both the cap on the leaf that parents the containers and the budget xdist workers are
sized from - so scheduling and containment agree on one number.

`Utils.physical_memory` reports HOST memory, which those containers do not get: on the
61.78 GiB runner it budgets 3 x 20 = 60 GiB for a `--dist=each` run inside a 40 GiB cap.

Capping the leaves moves an overrun out of the host's global scope, where the job's dmesg scan
saw it, into a cgroup, where nothing looked - so the reporting tests below matter as much as the
sizing ones: without them the change would silently turn a resource kill into a green job.

See ClickHouse/ClickHouse#112625.
"""

import ast
import inspect
import os
import re
import subprocess
import sys
import textwrap

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/job_configs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the
# path for `import praktika` to resolve to `ci/praktika`.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.defs.job_configs import (
    INTEGRATION_DIND_DAEMON_RESERVE,
    INTEGRATION_DIND_INIT_LIMIT,
    INTEGRATION_DIND_INIT_RESERVE,
    INTEGRATION_DIND_ROOT_RESERVE,
    INTEGRATION_NESTED_BUDGET,
    LIMITED_MEM,
    common_integration_test_job_config,
)
from ci.jobs.integration_test_job import (
    DIND_JOB_CGROUP_OOM,
    DIND_LEAF_MEANINGS,
    HOST_OOM_DMESG_PATTERNS,
    LATE_DMESG_LOG,
    MAX_MEM_PER_WORKER,
    MAX_MEM_PER_WORKER_DIST_EACH,
    OOM_DMESG_MARKERS,
    PREFETCH_PARALLEL_PULLS,
    TIMED_OUT_ARCHIVE_TIMEOUT,
    dind_leaf_oom_in_dmesg,
    dind_leaf_root,
    dind_unreportable_ooms,
    finalize_llvm_coverage_status,
    job_cgroup_oom,
    leaf_oom_report,
    leaf_oom_results,
    leaf_peak_usage,
    nested_budget_gb,
    planned_workers,
    prefetch_failure_result,
    print_leaf_peak_usage,
    print_timeout_diagnostics,
    pytest_workers,
    report_late_leaf_ooms,
    worker_plan,
)
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

GIB = 1024**3

# Runner labels of `common_integration_test_job_config` with their vCPU counts. No memory size
# is asserted: nothing in the repo maps a label to one (`RunnerLabels` in ci/defs/defs.py is
# label strings only), so the invariants below are checked across a range of plausible sizes
# instead of pinning an unverifiable figure.
CARRIERS = [
    ("AMD_MEDIUM", 16),
    ("ARM_MEDIUM", 16),
    ("AMD_SMALL_MEM", 8),
    ("ARM_SMALL_MEM", 8),
]
# Physical sizes the arithmetic must hold for, smallest first. 61 GiB is the size observed on
# the carrier that reported the failure in #112625.
SUPPORTED_PHYSICAL_GB = [16, 24, 32, 61]
# The size the CI carriers report, for the invariants that are specific to them.
CI_CARRIER_PHYSICAL_GB = 61

_DIND_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "jobs", "scripts", "docker_in_docker.sh"
)
_JOB_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "jobs", "integration_test_job.py"
)


def _budget_for(physical):
    """`(LIMITED_MEM, INTEGRATION_NESTED_BUDGET)` as `job_configs.py` computes them on a host of
    `physical` bytes.

    Re-imports the real module against a faked `Utils.physical_memory` rather than restating the
    arithmetic here: a local mirror would satisfy every assertion below while production computed
    something else, which is exactly the class of oracle this file exists to avoid. `praktika` is
    reloaded too, because `job_configs` imports `Utils` from there by its short name.
    """
    import importlib

    import praktika.utils

    import ci.defs.job_configs as cfg

    real = Utils.physical_memory
    faked = staticmethod(lambda: physical)
    Utils.physical_memory = faked
    praktika.utils.Utils.physical_memory = faked
    try:
        reloaded = importlib.reload(cfg)
        return reloaded.LIMITED_MEM, reloaded.INTEGRATION_NESTED_BUDGET
    finally:
        Utils.physical_memory = real
        praktika.utils.Utils.physical_memory = real
        importlib.reload(cfg)


def _init_reserve_for(physical):
    """`INTEGRATION_DIND_INIT_RESERVE` as `job_configs.py` computes it on a host of `physical`
    bytes.

    The reserve is a function of the job limit, so the imported constant describes the host
    running pytest; comparing it against a faked host's budget mixes two different machines.
    """
    import importlib

    import praktika.utils

    import ci.defs.job_configs as cfg

    real = Utils.physical_memory
    faked = staticmethod(lambda: physical)
    Utils.physical_memory = faked
    praktika.utils.Utils.physical_memory = faked
    try:
        return importlib.reload(cfg).INTEGRATION_DIND_INIT_RESERVE
    finally:
        Utils.physical_memory = real
        praktika.utils.Utils.physical_memory = real
        importlib.reload(cfg)


def _init_limit_for(physical):
    """`INTEGRATION_DIND_INIT_LIMIT` as `job_configs.py` computes it on a host of `physical`
    bytes.

    Same reason as `_init_reserve_for`: a test that overrides the reserves has to pass the
    limit production would pair with them, or the script rightly refuses the mismatch.
    """
    import importlib

    import praktika.utils

    import ci.defs.job_configs as cfg

    real = Utils.physical_memory
    faked = staticmethod(lambda: physical)
    Utils.physical_memory = faked
    praktika.utils.Utils.physical_memory = faked
    try:
        return importlib.reload(cfg).INTEGRATION_DIND_INIT_LIMIT
    finally:
        Utils.physical_memory = real
        praktika.utils.Utils.physical_memory = real
        importlib.reload(cfg)


# Hosts too small to hold the 11 GiB of reserves. The job refuses on these rather than running
# uncontained; the tests below pin that it refuses for the RIGHT reason.
UNSUPPORTED_PHYSICAL_GB = [8, 13]


# --- the static invariants that keep the outer limit from being the OOM scope -------------


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
def test_reserves_sum_to_the_job_limit(physical_gb):
    """Documents the intent. `INTEGRATION_NESTED_BUDGET` is defined as the remainder, so this
    is an identity and cannot fail - the invariants that can are below."""
    limited, nested = _budget_for(physical_gb * GIB)
    total = (
        INTEGRATION_DIND_ROOT_RESERVE
        + _init_reserve_for(physical_gb * GIB)
        + INTEGRATION_DIND_DAEMON_RESERVE
        + nested
    )
    assert total <= limited


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
def test_reserves_leave_a_usable_container_budget(physical_gb):
    """The failure mode an identity misses: a reserve that quietly takes a third of the budget.

    Sized through `_budget_for` rather than the importing host: below the reserve floor the
    budget clamps to 0 by design, so reading the live value failed this test on exactly the
    small hosts whose contract is to refuse cleanly.
    """
    limited, nested = _budget_for(physical_gb * GIB)
    assert nested > 0
    # `/init` takes a bounded share, so extra host memory no longer reaches the containers 1:1;
    # what must still hold is that it reaches them MONOTONICALLY and keeps the majority, or a
    # reserve that tracks the host could absorb an arbitrary amount of it.
    bigger_limited, bigger_nested = _budget_for((physical_gb + 8) * GIB)
    gained = bigger_nested - nested
    assert 0 < gained <= bigger_limited - limited
    assert gained * 2 > bigger_limited - limited


def test_the_carrier_budget_fits_a_dist_each_worker():
    """Sharper than the ratio above, and carrier-specific: 20 GiB per `--dist=each` worker only
    has to fit on the size the runners actually are. A 32 GiB host leaves 19 GiB and legitimately
    runs one smaller worker (`worker_plan` warns and reports what it really gets)."""
    _, nested = _budget_for(CI_CARRIER_PHYSICAL_GB * GIB)
    assert nested // GIB >= MAX_MEM_PER_WORKER_DIST_EACH


# The cap the carrier's `/init` was observed to exhaust. Every measurement of the demand itself is
# censored at this value - the leaf's own `total_rss` sat at 7.89-7.91 GiB against an 8388608 kB
# limit while the kernel killed to hold it - so what is known is that the harness wants MORE than
# this, not how much more.
CARRIER_EXHAUSTED_INIT_GIB = 8


def test_the_carrier_init_reserve_exceeds_the_cap_the_harness_exhausted():
    """The `/init` cap must exceed the one the harness was observed to exhaust.

    `helpers/cluster.py` gives every node a host-side `clickhouse-client`, and
    `helpers/iceberg_utils.py` runs Spark with a `local` master, so the per-test clients and the
    driver JVM are charged to `/init` and not to a container. That is what a flat 8 GiB missed.

    A lower bound rather than a target: the demand was never observed uncensored, so this asserts
    the direction the evidence supports and leaves the magnitude to `leaf_peak_usage`, which
    reports it from the next run.
    """
    assert (
        _init_reserve_for(CI_CARRIER_PHYSICAL_GB * GIB) // GIB
        > CARRIER_EXHAUSTED_INIT_GIB
    )


def test_the_init_reserve_tracks_the_job_limit_between_its_bounds():
    """The reserve must scale, or the same overrun returns on the next larger carrier.

    Pinned by ratio rather than by value: what makes it correct is that a bigger job limit yields
    a bigger harness allowance, up to the point where taking more would cost the containers a
    worker.
    """
    small = _init_reserve_for(24 * GIB)
    carrier = _init_reserve_for(CI_CARRIER_PHYSICAL_GB * GIB)
    assert small < carrier
    # Bounded at both ends: the floor keeps the smallest supported host runnable, and the cap
    # keeps the reserve from growing without limit on a large host.
    assert _init_reserve_for(16 * GIB) == _init_reserve_for(24 * GIB)
    assert _init_reserve_for(256 * GIB) == carrier


def test_the_init_reserve_does_not_cost_the_carrier_a_worker():
    """The reserve comes out of the container budget, which also sets xdist concurrency, so a
    reserve chosen without this check silently halves the shard's parallelism."""
    _, nested = _budget_for(CI_CARRIER_PHYSICAL_GB * GIB)
    for dist_each in (False, True):
        with_reserve = pytest_workers(nested // GIB, 16, dist_each=dist_each)
        # The same carrier under the flat 8 GiB reserve this replaced.
        flat = _budget_for(CI_CARRIER_PHYSICAL_GB * GIB)[0] - (
            INTEGRATION_DIND_ROOT_RESERVE + 8 * GIB + INTEGRATION_DIND_DAEMON_RESERVE
        )
        assert with_reserve == pytest_workers(flat // GIB, 16, dist_each=dist_each)


@pytest.mark.parametrize(
    "name,value",
    [
        ("root", INTEGRATION_DIND_ROOT_RESERVE),
        ("init", INTEGRATION_DIND_INIT_RESERVE),
        ("daemon", INTEGRATION_DIND_DAEMON_RESERVE),
    ],
)
def test_every_reserve_is_positive(name, value):
    assert value > 0


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
def test_budget_ordering(physical_gb):
    limited, nested = _budget_for(physical_gb * GIB)
    assert nested < limited < physical_gb * GIB + 1


def test_daemon_reserve_covers_the_shims_of_a_wide_shard():
    """The leaf holds dockerd, containerd and one containerd-shim per nested container, so it
    scales with concurrency: measured 89 MiB at rest and 380 MiB at 32 containers. The widest
    shard is 3 workers x 18 `add_instance` nodes, and a 53 MB leaf left dockerd unable to boot,
    so this is an absolute floor with margin over the extrapolation, not a fraction."""
    at_rest_mib, per_container_mib = 114, 8.5
    widest_shard_containers = 3 * 18
    projected = at_rest_mib + per_container_mib * widest_shard_containers
    assert INTEGRATION_DIND_DAEMON_RESERVE >= 2 * projected * 1024**2


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
@pytest.mark.parametrize("carrier,ncpu", CARRIERS)
@pytest.mark.parametrize("dist_each", [False, True], ids=["loadfile", "dist_each"])
def test_worker_sizing_fits_the_container_budget(physical_gb, carrier, ncpu, dist_each):
    """Whatever the carrier's size, the planned workers must fit the leaf that holds them.

    This is the invariant a hard-coded 61 GiB carrier could not express: nothing in the repo
    maps a runner label to a memory size, so if a `*_SMALL_MEM` row were also small in memory,
    an oracle pinned to 61 GiB would pass while the real carrier overcommitted.
    """
    limited, nested = _budget_for(physical_gb * GIB)
    assert nested > 0, f"{physical_gb} GiB leaves no container budget"
    mem_gb = nested // GIB
    workers, gb_per_worker = worker_plan(mem_gb, ncpu, dist_each=dist_each)
    assert workers >= 1
    assert workers * gb_per_worker <= mem_gb


def test_dist_each_overcommits_when_sized_from_host_memory():
    """The defect, on the reported runner: host-derived sizing budgets more than the leaf."""
    _, nested = _budget_for(61 * GIB)
    host_workers = pytest_workers(61 * GIB // GIB, 16, dist_each=True)
    assert host_workers * MAX_MEM_PER_WORKER_DIST_EACH > nested // GIB


@pytest.mark.parametrize("budget_gb", [-4, 0])
def test_pytest_workers_never_returns_less_than_one(budget_gb):
    """A negative budget made the floor return -1, because -1 is truthy."""
    for dist_each in (False, True):
        assert pytest_workers(budget_gb, 16, dist_each=dist_each) >= 1
        workers, gb = worker_plan(budget_gb, 16, dist_each=dist_each)
        assert workers == 1 and gb >= 0


def test_undersized_carrier_reports_the_memory_a_worker_actually_gets():
    """The smallest supported carrier cannot hold one modeled worker, so the plan must report
    the real per-worker share rather than the model figure. Without the cap this returns
    `MAX_MEM_PER_WORKER` and silently claims 11 GiB out of a 3 GiB budget."""
    _, nested = _budget_for(min(SUPPORTED_PHYSICAL_GB) * GIB)
    mem_gb = nested // GIB
    workers, gb_per_worker = worker_plan(mem_gb, 16, dist_each=False)
    assert workers == 1
    assert gb_per_worker < MAX_MEM_PER_WORKER
    assert workers * gb_per_worker <= mem_gb


# --- hosts too small for the reserves -----------------------------------------------------
# The reserves are absolute, so below 13 GiB there is nothing left over. That is a refusal, not
# a silent uncontained run - but it has to refuse for the reason the operator can act on, and
# `SUPPORTED_PHYSICAL_GB` stops one step above the break, so nothing covered it.


@pytest.mark.parametrize("physical_gb", UNSUPPORTED_PHYSICAL_GB)
def test_a_small_host_never_gets_a_negative_budget(physical_gb):
    """A negative reaches `docker_in_docker.sh`'s byte-count validator before its budget check,
    so the operator is told the variable is malformed. Clamped to zero, which is a well-formed
    byte count, so the accurate refusal fires instead."""
    _, nested = _budget_for(physical_gb * GIB)
    assert nested == 0
    assert re.fullmatch(r"[0-9]+", str(nested)), "must pass the shell byte-count validator"


@pytest.mark.parametrize("physical_gb", UNSUPPORTED_PHYSICAL_GB)
def test_a_small_host_refuses_naming_the_host_size_and_the_reserves(tmp_path, physical_gb):
    """End to end through the real script: the message must name what the operator can change."""
    limited, nested = _budget_for(physical_gb * GIB)
    init_reserve = _init_reserve_for(physical_gb * GIB)
    rc, out, cg = _run_containment(
        tmp_path,
        job_mem=limited,
        env_overrides={
            "CI_DIND_ROOT_RESERVE": str(INTEGRATION_DIND_ROOT_RESERVE),
            "CI_DIND_INIT_RESERVE": str(init_reserve),
            "CI_DIND_INIT_LIMIT": str(_init_limit_for(physical_gb * GIB)),
            "CI_DIND_DAEMON_RESERVE": str(INTEGRATION_DIND_DAEMON_RESERVE),
            "CI_DIND_NESTED_BUDGET": str(nested),
        },
    )
    assert rc == 3, out
    # Not the byte-count message, which is what a negative used to produce.
    assert "expected a byte count" not in out, out
    assert "leaving the test containers nothing" in out, out
    assert f"{limited // GIB} GiB job limit" in out, out
    reserves_gib = (
        INTEGRATION_DIND_ROOT_RESERVE + init_reserve + INTEGRATION_DIND_DAEMON_RESERVE
    ) // GIB
    assert f"reserves need {reserves_gib} GiB" in out, out
    assert "larger host" in out and "CI_DIND_REQUIRE_CGROUP_CONTAINMENT" in out, out
    assert not (cg / "init").exists(), "refused, yet it created leaves"


def test_the_smallest_supported_host_still_runs(tmp_path):
    """The other side of the boundary: 16 GiB must NOT refuse, so the clamp cannot creep up."""
    limited, nested = _budget_for(min(SUPPORTED_PHYSICAL_GB) * GIB)
    assert nested > 0
    rc, out, _ = _run_containment(
        tmp_path,
        job_mem=limited,
        env_overrides={
            "CI_DIND_ROOT_RESERVE": str(INTEGRATION_DIND_ROOT_RESERVE),
            "CI_DIND_INIT_RESERVE": str(
                _init_reserve_for(min(SUPPORTED_PHYSICAL_GB) * GIB)
            ),
            "CI_DIND_INIT_LIMIT": str(
                _init_limit_for(min(SUPPORTED_PHYSICAL_GB) * GIB)
            ),
            "CI_DIND_DAEMON_RESERVE": str(INTEGRATION_DIND_DAEMON_RESERVE),
            "CI_DIND_NESTED_BUDGET": str(nested),
        },
    )
    assert rc == 0, out
    assert "containment active" in out


def test_the_ci_carriers_are_unaffected_by_the_clamp():
    """The clamp must be inert on every size the CI carriers actually have (61.65-61.78 GiB
    measured across all four runner labels), so it cannot mask a real arithmetic error there."""
    for physical_gb in SUPPORTED_PHYSICAL_GB:
        limited, nested = _budget_for(physical_gb * GIB)
        unclamped = (
            limited
            - INTEGRATION_DIND_ROOT_RESERVE
            - _init_reserve_for(physical_gb * GIB)
            - INTEGRATION_DIND_DAEMON_RESERVE
        )
        assert nested == unclamped > 0, physical_gb


# The peak `/init` reached on the two carrier shards that were killed, and the peak on the four
# that survived the same run. Measured on #112984 head 85167349, all six shards identically
# configured (16 vCPU, 40 GiB container budget, 3 xdist workers): the survivors sat at
# 11.47-12.47 GiB while the two that died were censored AT the 16 GiB cap. So the demand is
# bursty, not a uniformly larger steady state, and the burst is one test's host-side client
# fan-out (60 concurrent `clickhouse` clients at ~250 MiB each under a sanitizer).
CARRIER_INIT_STEADY_PEAK_GIB = 12.47
CARRIER_INIT_KILLED_AT_GIB = 16


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
def test_the_init_limit_exceeds_its_share_of_the_budget(physical_gb):
    """Capping `/init` at its share killed shards whose aggregate peak still fit the job limit,
    so the limit must be the larger of the two or the kill returns."""
    assert _init_limit_for(physical_gb * GIB) > _init_reserve_for(physical_gb * GIB)


def test_the_carrier_init_limit_clears_the_burst_that_killed_the_shards():
    """A share-sized cap is what the carriers had when they died, so the limit must clear the
    censored figure by a real margin rather than merely differing from the share."""
    limit_gib = _init_limit_for(CI_CARRIER_PHYSICAL_GB * GIB) / GIB
    assert limit_gib > CARRIER_INIT_KILLED_AT_GIB
    # Room for the burst on top of the observed steady state, not just above the old cap.
    assert limit_gib >= CARRIER_INIT_STEADY_PEAK_GIB + CARRIER_INIT_KILLED_AT_GIB


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
def test_the_written_caps_overcommit_the_job_limit(physical_gb, tmp_path):
    """The cost of sizing `/init` for a burst: the caps no longer partition the job limit, so the
    parent can be what breaches. Accepted deliberately, and what makes it acceptable is that the
    breach is reported - so assert the overcommit and that report together, or the overcommit
    could grow while the row that covers it was quietly dropped."""
    limited, nested = _budget_for(physical_gb * GIB)
    written = (
        _init_limit_for(physical_gb * GIB) + INTEGRATION_DIND_DAEMON_RESERVE + nested
    )
    assert written > limited, physical_gb
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, _ = leaf_oom_report(_REQUIRED, _JOB_CGROUP_BREACH, cgroup_root=tree)
    assert [r.name for r in results] == [DIND_JOB_CGROUP_OOM]


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
def test_the_init_limit_still_fits_beside_the_leaves_it_overlaps(physical_gb):
    """The limit overlaps `/docker`'s reserve on purpose, so what bounds it is the leaves it does
    NOT overlap: it may not push the root and daemon reserves past the job limit."""
    limited, _ = _budget_for(physical_gb * GIB)
    total = (
        _init_limit_for(physical_gb * GIB)
        + INTEGRATION_DIND_ROOT_RESERVE
        + INTEGRATION_DIND_DAEMON_RESERVE
    )
    assert total <= limited, physical_gb


@pytest.mark.parametrize("physical_gb", SUPPORTED_PHYSICAL_GB)
def test_the_init_limit_does_not_change_worker_concurrency(physical_gb):
    """`/init`'s cap and the container budget are decoupled on purpose: the budget xdist sizes
    workers from stays the share, so raising the cap cannot change concurrency."""
    limited, nested = _budget_for(physical_gb * GIB)
    assert nested == (
        limited
        - INTEGRATION_DIND_ROOT_RESERVE
        - _init_reserve_for(physical_gb * GIB)
        - INTEGRATION_DIND_DAEMON_RESERVE
    )


def test_the_job_passes_the_init_limit_to_the_script():
    """The constant is inert unless it reaches `docker_in_docker.sh`, whose validator refuses
    without it."""
    assert (
        f"--env=CI_DIND_INIT_LIMIT={INTEGRATION_DIND_INIT_LIMIT}"
        in common_integration_test_job_config.run_in_docker
    )


# --- nested_budget_gb: the wiring, assertable on any host --------------------------------
# Sources are injected rather than read from the process, so these discriminate even where the
# job's env is absent and host memory happens to equal the budget.


def test_budget_comes_from_the_env_not_host_memory():
    gb = nested_budget_gb(
        env={"CI_DIND_NESTED_BUDGET": str(49 * GIB)},
        physical_memory=lambda: 755 * GIB,
    )
    assert gb == 49


def test_falls_back_to_host_memory_without_the_env():
    """Local runs and jobs that set no memory limit keep today's behavior."""
    assert nested_budget_gb(env={}, physical_memory=lambda: 61 * GIB) == 61


def test_an_empty_env_value_is_refused_rather_than_falling_back():
    """`--param CI_DIND_NESTED_BUDGET=` overwrites the job's value with an empty string, and
    the host-memory fallback is the overcommit this sizing exists to remove. Only an absent
    key means no budget was set; the script refuses an empty one for the same reason.
    """
    with pytest.raises(ValueError):
        nested_budget_gb(
            env={"CI_DIND_NESTED_BUDGET": ""}, physical_memory=lambda: 61 * GIB
        )


def test_malformed_env_value_raises_rather_than_overcommitting():
    """Silently falling back here would restore the host-memory overcommit."""
    with pytest.raises(ValueError):
        nested_budget_gb(
            env={"CI_DIND_NESTED_BUDGET": "not-a-number"},
            physical_memory=lambda: 61 * GIB,
        )


# Values the daemon is refused for. The last two are digits to `str.isdigit()` and to `int()`
# but not to the script's `[0-9]` case, so they are what separates the two spellings.
_NOT_BYTE_COUNTS = [
    "-1",
    "-1073741824",
    "0",
    " 5368709120 ",
    "+1073741824",
    "1_073_741_824",
    "٣",
    "1٣",
    # All digits, but no host offers them. 2**63 and above cannot be parsed by Bash's signed
    # arithmetic at all; 2**63-1 parses and then wraps the reserve sum negative, which is why
    # each reserve is now bounded by the job limit before being summed.
    str(2**63),
    "1" + "0" * 30,
    str(2**63 - 1),
    # `--param CI_DIND_NESTED_BUDGET=` writes this over the job's value. The script refuses it;
    # falling back to host memory here is exactly the overcommit this sizing removes.
    "",
]


def _job_env(value):
    """The budget under test alongside the job limit the script would be given with it."""
    return {"CI_DIND_NESTED_BUDGET": value, "CI_DIND_JOB_MEM": str(8 * GIB)}


@pytest.mark.parametrize("value", _NOT_BYTE_COUNTS)
def test_a_budget_the_daemon_would_refuse_raises_here_too(value):
    with pytest.raises(ValueError):
        nested_budget_gb(env=_job_env(value), physical_memory=lambda: 61 * GIB)


@pytest.mark.parametrize("value", _NOT_BYTE_COUNTS + [str(3 * GIB)])
def test_the_two_validators_agree_on_the_same_value(tmp_path, value):
    """The sizing reads the budget before the daemon starts, and an already-running daemon
    skips the script's check entirely, so neither validator backs the other up. Pin them to
    one verdict here, or a later edit to either contract reopens the gap silently.
    """
    rc, out, _ = _run_containment(
        tmp_path, env_overrides={"CI_DIND_NESTED_BUDGET": value}, job_mem=8 * GIB
    )
    try:
        # Both sides get the same job limit, or the comparison is between two questions.
        nested_budget_gb(env=_job_env(value), physical_memory=lambda: 61 * GIB)
        python_refused = False
    except ValueError:
        python_refused = True
    assert python_refused == (rc != 0), (
        f"[{value}]: the script exited {rc} but Python "
        f"{'refused' if python_refused else 'accepted'} it: {out}"
    )


@pytest.mark.parametrize("value", _NOT_BYTE_COUNTS)
def test_the_refusal_precedes_the_worker_count(value):
    """`planned_workers` floors at one worker, so a rejected budget that only warned would
    still schedule a run. Assert the raise reaches the caller, not just the parser.
    """
    with pytest.raises(ValueError):
        planned_workers(
            None,
            nested_budget_gb(env=_job_env(value), physical_memory=lambda: 61 * GIB),
            16,
            False,
        )


def test_the_budget_follows_the_environment_set_after_import():
    """The sizing must see a budget written after the module was imported, which is when
    `--param` writes it. Reading it at import instead is invisible to a test that sets the
    variable first and then reloads, so the env is set here with the module already loaded.
    """
    import ci.jobs.integration_test_job as job

    distinct = str(Utils.physical_memory() // 2)
    os.environ["CI_DIND_NESTED_BUDGET"] = distinct
    try:
        assert job.nested_budget_gb() == round(int(distinct) // GIB, 1)
        assert job.nested_budget_gb() != round(Utils.physical_memory() // GIB, 1)
    finally:
        del os.environ["CI_DIND_NESTED_BUDGET"]


# --- the caller that turns the budget into a worker count ---------------------------------
# The test above pins the module global; these pin the decision `main` actually makes with it.
# Without them a call site handing `Utils.physical_memory()` back to the sizing passes the whole
# file, which is precisely the overcommit this change exists to remove.


def test_the_worker_count_follows_the_budget_not_host_memory():
    """The reported defect, at the call site: on the carrier that reported #112625, 61 GiB of
    host memory plans 3 `--dist=each` workers at 20 GiB each, which does not fit the container
    budget. Sized from the budget it plans 2, which does.

    The budget is asserted to be smaller than the host rather than pinned to a literal: it is a
    remainder after the reserves, so a literal here restates their arithmetic and turns any
    reserve change into a failure of this test, which is about the call site."""
    _, nested = _budget_for(61 * GIB)
    budget_gb = nested // GIB
    assert 0 < budget_gb < 61

    from_budget = planned_workers(None, budget_gb, 16, dist_each=True)
    from_host = planned_workers(None, 61, 16, dist_each=True)

    assert from_host == 3 and from_host * MAX_MEM_PER_WORKER_DIST_EACH > budget_gb
    assert from_budget == 2 and from_budget * MAX_MEM_PER_WORKER_DIST_EACH <= budget_gb
    assert from_budget != from_host


def test_the_loadfile_worker_count_also_follows_the_budget():
    """The other half of the matrix. Uses 24 GiB rather than 61: at 61 the two figures happen to
    plan the same 3 workers (33 GiB fits either way), so a 61 GiB oracle here would pass on a
    call site sized from host memory."""
    _, nested = _budget_for(24 * GIB)
    budget_gb = nested // GIB
    assert budget_gb == 11

    from_budget = planned_workers(None, budget_gb, 16, dist_each=False)
    from_host = planned_workers(None, 24, 16, dist_each=False)

    assert from_host == 2 and from_host * MAX_MEM_PER_WORKER > budget_gb
    assert from_budget == 1 and from_budget * MAX_MEM_PER_WORKER <= budget_gb


@pytest.mark.parametrize("dist_each", [False, True], ids=["loadfile", "dist_each"])
def test_an_explicit_workers_argument_still_wins(dist_each):
    """`--workers` is an operator override and must not be overridden by the budget."""
    assert planned_workers(7, 49, 16, dist_each=dist_each) == 7


def _main_calls(func_name):
    """Every call to `func_name` inside `main`, as `(source, arg_sources)` pairs.

    Parsed rather than string-sliced so a nested call in an argument cannot end the match early.
    `main` itself cannot be invoked here: it starts a Docker daemon.
    """
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    return [
        (ast.unparse(n), [ast.unparse(a) for a in n.args + [k.value for k in n.keywords]])
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == func_name
    ]


def test_main_derives_its_worker_count_through_the_caller():
    """`main` must size workers through `planned_workers` from the BUDGET.

    Paired with `test_the_worker_count_is_read_after_param_injection`, which pins WHEN the budget
    is read: that one catches a read that happens too early, this one catches the call site
    sourcing it from host memory. Either alone leaves the other half free to restore the
    overcommit.
    """
    calls = _main_calls("planned_workers")
    assert len(calls) == 1, f"expected exactly one sizing call site, found {len(calls)}"
    call, args = calls[0]
    assert (
        "nested_budget_gb()" in args
    ), f"main sizes workers from something other than the budget: {call}"
    assert "physical_memory" not in call, f"main sizes workers from host memory: {call}"


# --- reporting an exhausted leaf ----------------------------------------------------------
# Capping the leaves moved the OOM out of the scope the job watched. These pin the replacement
# report, and the discrimination it has to make: a container reaching the `mem_limit` its own
# module asked for is a pre-existing outcome that some modules tolerate on purpose, so it must
# NOT become a job error.


def _leaf_tree(root, **events):
    """A fake cgroup tree at `root`. `events[leaf]` is that leaf's `memory.events.local` body."""
    for leaf in DIND_LEAF_MEANINGS:
        (root / leaf).mkdir(parents=True)
        if leaf in events:
            (root / leaf / "memory.events.local").write_text(
                events[leaf], encoding="utf-8"
            )
    return root


_NO_OOM = "low 0\nhigh 0\nmax 12\noom 0\noom_kill 0\noom_group_kill 0\n"
_OWN_LIMIT_OOM = "low 0\nhigh 0\nmax 55\noom 1\noom_kill 0\noom_group_kill 0\n"


def test_exhausted_container_budget_is_reported_as_an_error(tmp_path):
    tree = _leaf_tree(
        tmp_path, docker=_OWN_LIMIT_OOM, init=_NO_OOM, dockerd=_NO_OOM
    )
    results = leaf_oom_results(cgroup_root=tree)
    assert len(results) == 1
    assert "/docker" in results[0].name
    assert results[0].status == Result.Status.ERROR


@pytest.mark.parametrize("leaf", sorted(DIND_LEAF_MEANINGS))
def test_each_leaf_is_named_in_its_own_result(tmp_path, leaf):
    tree = _leaf_tree(
        tmp_path / f"tree_{leaf}",
        **{
            other: (_OWN_LIMIT_OOM if other == leaf else _NO_OOM)
            for other in DIND_LEAF_MEANINGS
        },
    )
    results = leaf_oom_results(cgroup_root=tree)
    assert [r.name for r in results] == [DIND_LEAF_MEANINGS[leaf]]


def test_no_oom_produces_no_result(tmp_path):
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    assert leaf_oom_results(cgroup_root=tree) == []


def test_a_container_hitting_its_own_mem_limit_is_not_reported(tmp_path):
    """Every test container carries a `mem_limit` (12g by default) and one module says verbatim
    that its container "might be killed by OOM killer but it is fine". Such a kill is charged
    to the container's own cgroup, so `/docker` sees it only in the AGGREGATING `memory.events`
    - measured: aggregating `oom_kill` moves, `memory.events.local`'s `oom` does not. Reading
    the aggregating counter would turn those tolerated kills into job errors."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    (tree / "docker" / "memory.events").write_text(
        "low 0\nhigh 0\nmax 58\noom 1\noom_kill 1\noom_group_kill 0\n", encoding="utf-8"
    )
    assert leaf_oom_results(cgroup_root=tree) == []


def test_missing_and_malformed_files_are_not_errors(tmp_path):
    """The permissive path creates no leaves at all, and a read can race a teardown."""
    empty = tmp_path / "empty"
    empty.mkdir()
    assert leaf_oom_results(cgroup_root=empty) == []
    tree = _leaf_tree(tmp_path / "bad", docker="oom not-a-number\n", init=_NO_OOM)
    assert leaf_oom_results(cgroup_root=tree) == []


# Real kernel lines. The two differ ONLY in `oom_memcg=`, which names the cgroup whose limit was
# breached: the leaf itself for a collective overrun, a per-container cgroup otherwise.
_SCOPE = "/system.slice/docker-747c6c58433774549e449fd3d7c6a3705aa2b8d22690137676e622c1074136b7.scope"
_COLLECTIVE_BREACH = (
    f"oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=56c72ce5,mems_allowed=0-5,"
    f"oom_memcg={_SCOPE}/docker,task_memcg={_SCOPE}/docker/56c72ce57ea5,task=python3,pid=2871442,uid=0\n"
    "Memory cgroup out of memory: Killed process 2871442 (python3) total-vm:7326816kB\n"
    # The reaper line the kernel may add after any kill, global or contained. Present here so
    # `test_host_oom_patterns_match_a_host_oom_and_not_a_cgroup_oom` cannot pass on a detector
    # that keys on it: without it that test holds while every leaf kill reports a host OOM too.
    "oom_reaper: reaped process 2871442 (python3), now anon-rss:0kB, file-rss:0kB\n"
).encode()
_OWN_MEM_LIMIT = (
    f"oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=e07a986b,mems_allowed=0-5,"
    f"oom_memcg={_SCOPE}/docker/e07a986b89ed,task_memcg={_SCOPE}/docker/e07a986b89ed,task=python3,pid=2870241,uid=0\n"
    "Memory cgroup out of memory: Killed process 2870241 (python3) total-vm:134604kB\n"
).encode()
_DAEMON_BREACH = (
    f"oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=8f1c40de,mems_allowed=0-5,"
    f"oom_memcg={_SCOPE}/dockerd,task_memcg={_SCOPE}/dockerd,task=dockerd,pid=2871003,uid=0\n"
    "Memory cgroup out of memory: Killed process 2871003 (dockerd) total-vm:2841236kB\n"
    "oom_reaper: reaped process 2871003 (dockerd), now anon-rss:0kB, file-rss:0kB\n"
).encode()
_TWO_LEAVES_BREACHED = _COLLECTIVE_BREACH + _DAEMON_BREACH
_HOST_OOM = (
    "oom-kill:constraint=CONSTRAINT_NONE,nodemask=(null),cpuset=user.slice,mems_allowed=0-5,"
    "global_oom,task_memcg=/user.slice/session-11023.scope,task=clickhouse-loca,pid=1247222,uid=0\n"
    "Out of memory: Killed process 1247222 (clickhouse-loca) total-vm:2311286376kB\n"
    "oom_reaper: reaped process 1247222 (clickhouse-loca), now anon-rss:0kB, file-rss:0kB\n"
).encode()


def test_dmesg_scan_sees_a_collective_breach_of_a_leaf():
    assert dind_leaf_oom_in_dmesg(_COLLECTIVE_BREACH) == {"docker"}


def test_dmesg_scan_names_every_breached_leaf():
    assert dind_leaf_oom_in_dmesg(_TWO_LEAVES_BREACHED) == {"docker", "dockerd"}


def test_dmesg_scan_ignores_a_container_reaching_its_own_mem_limit():
    """The pre-existing, sometimes deliberate case: same two substrings, different `oom_memcg`."""
    assert dind_leaf_oom_in_dmesg(_OWN_MEM_LIMIT) == set()


def test_dmesg_scan_ignores_a_host_oom():
    """Host OOMs are reported by `HOST_OOM_DMESG_PATTERNS`, not as a leaf overrun."""
    assert dind_leaf_oom_in_dmesg(_HOST_OOM) == set()


def test_host_oom_patterns_match_a_host_oom_and_not_a_cgroup_oom():
    assert any(p in _HOST_OOM for p in HOST_OOM_DMESG_PATTERNS)
    for memcg in (_COLLECTIVE_BREACH, _DAEMON_BREACH, _OWN_MEM_LIMIT):
        assert not any(p in memcg for p in HOST_OOM_DMESG_PATTERNS)


def test_host_oom_patterns_do_not_key_on_the_reaper_line():
    """The kernel prints `oom_reaper` for a contained kill too, so keying on it would report a
    host OOM on top of every leaf row - the leaves are capped now, so those are routine."""
    assert not any(b"oom_reaper" in p for p in HOST_OOM_DMESG_PATTERNS)
    # The reserve: a global OOM stays detectable without it, so nothing is traded away.
    reaperless = _HOST_OOM.replace(b"oom_reaper: reaped process 1247222 (clickhouse-loca)", b"")
    assert any(p in reaperless for p in HOST_OOM_DMESG_PATTERNS)


# --- the caller that turns those two signals into rows ------------------------------------
# The counters and the dmesg backstop have their own tests above, but nothing there reaches the
# wiring that reports them: deleting it entirely leaves every assertion above passing while no
# leaf OOM is reported at all, which is the pre-fix condition restored invisibly.

_REQUIRED = {"CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1"}
_CLEAN_DMESG = b"[Sat Aug  1 00:00:00 2026] nothing interesting here\n"


def test_a_breached_leaf_is_reported_and_the_dmesg_log_attached(tmp_path):
    tree = _leaf_tree(tmp_path, docker=_OWN_LIMIT_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, attach_dmesg = leaf_oom_report(_REQUIRED, _CLEAN_DMESG, cgroup_root=tree)
    assert [r.name for r in results] == [DIND_LEAF_MEANINGS["docker"]]
    assert results[0].status == Result.Status.ERROR
    assert attach_dmesg is True


def test_a_dmesg_collective_breach_is_the_fallback_when_the_counters_are_clean(tmp_path):
    """A mid-run daemon restart recreates the leaf and zeroes its counters, so the scan backs
    the counters up - and names the same leaf the counter path would have named."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, attach_dmesg = leaf_oom_report(_REQUIRED, _DAEMON_BREACH, cgroup_root=tree)
    assert [r.name for r in results] == [DIND_LEAF_MEANINGS["dockerd"]]
    assert results[0].status == Result.Status.ERROR
    assert attach_dmesg is True


def test_a_counter_breach_does_not_suppress_another_leaf_in_dmesg(tmp_path):
    """The two signals are per leaf, so one leaf's counter firing must not hide another's:
    a recreated `/dockerd` reports through dmesg while `/docker`'s counter holds."""
    tree = _leaf_tree(tmp_path, docker=_OWN_LIMIT_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, attach_dmesg = leaf_oom_report(
        _REQUIRED, _TWO_LEAVES_BREACHED, cgroup_root=tree
    )
    assert sorted(r.name for r in results) == sorted(
        (DIND_LEAF_MEANINGS["docker"], DIND_LEAF_MEANINGS["dockerd"])
    )
    assert attach_dmesg is True


def test_the_fallback_reports_every_leaf_dmesg_names(tmp_path):
    """Two leaves recreated, so the fallback carries both rows on its own."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, _ = leaf_oom_report(_REQUIRED, _TWO_LEAVES_BREACHED, cgroup_root=tree)
    assert sorted(r.name for r in results) == sorted(
        (DIND_LEAF_MEANINGS["docker"], DIND_LEAF_MEANINGS["dockerd"])
    )


def test_a_leaf_in_both_signals_yields_one_row(tmp_path):
    """Its counter fired and dmesg names it: one breach, one row."""
    tree = _leaf_tree(tmp_path, docker=_OWN_LIMIT_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, _ = leaf_oom_report(_REQUIRED, _COLLECTIVE_BREACH, cgroup_root=tree)
    assert [r.name for r in results] == [DIND_LEAF_MEANINGS["docker"]]


def test_a_container_reaching_its_own_mem_limit_produces_no_row(tmp_path):
    """The tolerated case must survive both signals at once: clean counters plus a dmesg line
    whose `oom_memcg` is a per-container cgroup."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    assert leaf_oom_report(_REQUIRED, _OWN_MEM_LIMIT, cgroup_root=tree) == ([], False)


def test_a_host_oom_is_not_reported_as_a_leaf_overrun(tmp_path):
    """`HOST_OOM_DMESG_PATTERNS` owns that row; this path must not double-report it."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    assert leaf_oom_report(_REQUIRED, _HOST_OOM, cgroup_root=tree) == ([], False)


# --- the job's own cgroup, which the leaf caps may collectively overcommit -----------------
# `/init` is capped for one test's burst while `/docker` keeps an average-sized budget, so the
# caps deliberately sum above the job limit and the parent can be what breaches. That kill is
# neither a leaf (its `oom_memcg` names no leaf) nor a host OOM (it is `CONSTRAINT_MEMCG`, and
# those patterns are global-only), so without this it would be reported by nothing at all.

# A breach above the leaves: the victim sits in a leaf, the cgroup that hit its cap is its parent.
_JOB_CGROUP_BREACH = (
    f"oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=56c72ce5,mems_allowed=0-5,"
    f"oom_memcg={_SCOPE},task_memcg={_SCOPE}/init,task=clickhouse,pid=2871442,uid=0\n"
    "Memory cgroup out of memory: Killed process 2871442 (clickhouse) total-vm:7326816kB\n"
).encode()
# The same breach with the victim inside a nested container. A parent OOM scans the whole subtree,
# so this is the usual shape, and it is the only one v1's counter can never report: that fallback
# counts victims of the cgroup they belong to, and this victim belongs to a child.
_JOB_CGROUP_BREACH_NESTED_VICTIM = (
    f"oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=e07a986b,mems_allowed=0-5,"
    f"oom_memcg={_SCOPE},task_memcg={_SCOPE}/docker/e07a986b89ed,task=clickhouse,pid=2870241,uid=0\n"
    "Memory cgroup out of memory: Killed process 2870241 (clickhouse) total-vm:134604kB\n"
).encode()


def test_a_job_cgroup_breach_is_reported(tmp_path):
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, attach_dmesg = leaf_oom_report(
        _REQUIRED, _JOB_CGROUP_BREACH, cgroup_root=tree
    )
    assert [r.name for r in results] == [DIND_JOB_CGROUP_OOM]
    assert results[0].status == Result.Status.ERROR
    assert attach_dmesg is True


def test_a_job_breach_with_a_nested_container_victim_is_reported(tmp_path):
    """A parent OOM scans the whole subtree, so the victim is usually a container rather than a
    leaf. Requiring a leaf victim would miss it, and on v1 nothing else can report it: that
    counter fallback only counts victims belonging to the cgroup itself."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    results, _ = leaf_oom_report(
        _REQUIRED, _JOB_CGROUP_BREACH_NESTED_VICTIM, cgroup_root=tree
    )
    assert [r.name for r in results] == [DIND_JOB_CGROUP_OOM]


def test_a_v1_job_cgroup_breach_with_a_nested_victim_is_reported(tmp_path):
    """The v1 arm of the case above: no `memory.events.local` anywhere, so dmesg is the only
    signal, and the root's `oom_control` is structurally 0 for a victim in a child."""
    tree = tmp_path
    for leaf in DIND_LEAF_MEANINGS:
        (tree / leaf).mkdir(parents=True)
        (tree / leaf / "memory.oom_control").write_text(
            "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n", encoding="utf-8"
        )
    (tree / "memory.oom_control").write_text(
        "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n", encoding="utf-8"
    )
    results, _ = leaf_oom_report(
        _REQUIRED, _JOB_CGROUP_BREACH_NESTED_VICTIM, cgroup_root=tree
    )
    assert [r.name for r in results] == [DIND_JOB_CGROUP_OOM]


def test_the_job_cgroups_own_counter_reports_it_too(tmp_path):
    """The counter arm, for a dmesg buffer the caller could not attribute to this run."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    (tree / "memory.events.local").write_text(_OWN_LIMIT_OOM, encoding="utf-8")
    results, _ = leaf_oom_report(_REQUIRED, b"", cgroup_root=tree)
    assert [r.name for r in results] == [DIND_JOB_CGROUP_OOM]


@pytest.mark.parametrize(
    "dmesg",
    [_COLLECTIVE_BREACH, _DAEMON_BREACH, _OWN_MEM_LIMIT, _HOST_OOM],
    ids=["collective-docker", "dockerd", "container-own-mem-limit", "host-oom"],
)
def test_a_leaf_or_container_kill_is_not_reported_as_a_job_breach(tmp_path, dmesg):
    """The negative controls that keep the parent row from firing on every kill: each of these
    already has an owner, and the owner/victim pair is what separates them."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    assert not job_cgroup_oom(dmesg, cgroup_root=tree)


def test_the_job_breach_detector_is_independent_of_the_cgroup_path_shape(tmp_path):
    """Production reports `/docker/<id>/init` while a systemd-scoped host reports
    `/system.slice/docker-<id>.scope/init`, so path shape cannot identify the parent."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    docker_shape = (
        "oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),"
        f"oom_memcg=/docker/{'0' * 64},task_memcg=/docker/{'0' * 64}/init,task=clickhouse,pid=1,uid=0\n"
    ).encode()
    assert job_cgroup_oom(docker_shape, cgroup_root=tree)


def test_a_clean_run_produces_no_row(tmp_path):
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    assert leaf_oom_report(_REQUIRED, _CLEAN_DMESG, cgroup_root=tree) == ([], False)


@pytest.mark.parametrize(
    "env",
    [{}, {"CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "0"}],
    ids=["absent", "zero"],
)
def test_without_the_request_nothing_is_reported_even_from_a_breached_tree(tmp_path, env):
    """The permissive contract, and the assertion that catches the whole block being deleted:
    without containment these paths are the HOST's cgroups, so the counters must not be read -
    yet with it, the identical tree must produce a row. Both halves, one fixture."""
    tree = _leaf_tree(
        tmp_path, docker=_OWN_LIMIT_OOM, init=_OWN_LIMIT_OOM, dockerd=_OWN_LIMIT_OOM
    )
    assert leaf_oom_report(env, _COLLECTIVE_BREACH, cgroup_root=tree) == ([], False)
    required, _ = leaf_oom_report(_REQUIRED, _COLLECTIVE_BREACH, cgroup_root=tree)
    assert len(required) == 3, "the same tree under containment must report every leaf"


def test_the_dmesg_log_is_attached_once_however_many_leaves_breached(tmp_path):
    """`create_from` stores `files` verbatim, so a per-leaf append would list the path 3 times."""
    tree = _leaf_tree(
        tmp_path, docker=_OWN_LIMIT_OOM, init=_OWN_LIMIT_OOM, dockerd=_OWN_LIMIT_OOM
    )
    results, attach_dmesg = leaf_oom_report(_REQUIRED, _CLEAN_DMESG, cgroup_root=tree)
    assert len(results) == 3
    assert attach_dmesg is True, "a bool, not a count: `main` appends the path once"


def test_main_reports_leaf_ooms_through_the_caller():
    """`main` must call `leaf_oom_report` and feed every row into `has_error` and `error_info`.

    Computing the rows is not enough: they have to reach the job status and the report. Deleting
    the block, or keeping the call while dropping the `has_error` assignment, reddens this.
    """
    calls = _main_calls("leaf_oom_report")
    assert len(calls) == 1, f"expected exactly one reporting call site, found {len(calls)}"
    _, args = calls[0]
    assert "os.environ" in args, "the containment request is not read from the environment"
    assert any("dmesg" in a for a in args), "the dmesg backstop is not wired in"

    # The loop that consumes those rows, located by the name the call binds them to.
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    loop = re.search(
        r"\n( +)for leaf_result in leaf_results:\n((?:\1 +.*\n|\n)+)", source
    )
    assert loop, "the leaf rows are computed but never consumed"
    body = loop.group(2)
    assert "test_results.append(leaf_result)" in body, "the rows never reach test_results"
    assert "has_error = True" in body, "the rows no longer set has_error"
    assert "error_info.append" in body, "the rows never reach error_info"


# --- from an exhausted leaf to a non-green JOB status --------------------------------------
# The results above only pin what `leaf_oom_results` returns. What the change actually promises
# is that the row reaches the job's status, and the coverage shards nearly broke that promise:
# their "do not block on test failures" rule called `set_success()` and cleared `has_error`, so
# the new `ERROR` (not a `FAIL`, so invisible to that rule's scan) was erased on all eight
# `amd_llvm_coverage` shards. These drive the decision itself, following the shape
# ci/tests/test_bugfix_validation_inverter.py uses for the sibling `OOM in dmesg` row.


def _job_status(children, has_error, is_llvm_coverage):
    """The final top-level status, as `main` derives it from its children.

    Mirrors `main`'s order over the three steps that can still change the status once the leaf
    row has been appended: `create_from`, the coverage rule, the all-infra clearing block, and
    `if has_error: set_error()`. The clearing block is included rather than elided because it is
    the other place an `ERROR` can be dropped - a leaf row carrying `INFRA` would be cleared
    there, so leaving it out would make these assertions weaker than the code they guard.
    """
    R = Result.create_from(name="job", results=list(children), status="", files=[])
    if is_llvm_coverage:
        has_error = finalize_llvm_coverage_status(R, has_error)
    if has_error:
        non_ok = [r for r in children if not r.is_ok()]
        if non_ok and all(r.has_label(Result.Label.INFRA) for r in non_ok):
            has_error = False
    if has_error:
        R.set_error().set_info("infrastructure/resource failure")
    return R


def _child(name, status, label=None):
    r = Result(name=name, status=status)
    if label:
        r.set_label(label)
    return r


_LEAF_OOM_ROW = DIND_LEAF_MEANINGS["docker"]


def test_an_exhausted_leaf_makes_a_coverage_shard_report_error():
    """The regression this pins: the row is an `ERROR`, the coverage rule only looks for `FAIL`,
    and `set_success()` plus `has_error = False` used to finish the job green."""
    R = _job_status(
        [_child("test_foo", Result.Status.OK), _child(_LEAF_OOM_ROW, Result.Status.ERROR)],
        has_error=True,
        is_llvm_coverage=True,
    )
    assert R.status == Result.Status.ERROR, R.info
    assert not R.is_success()


def test_an_exhausted_leaf_makes_an_ordinary_shard_report_error():
    """The non-coverage control: same input, and it was already correct."""
    R = _job_status(
        [_child("test_foo", Result.Status.OK), _child(_LEAF_OOM_ROW, Result.Status.ERROR)],
        has_error=True,
        is_llvm_coverage=False,
    )
    assert R.status == Result.Status.ERROR
    assert not R.is_success()


def test_a_clean_coverage_shard_still_reports_success():
    R = _job_status(
        [_child("test_foo", Result.Status.OK)], has_error=False, is_llvm_coverage=True
    )
    assert R.status == Result.Status.OK
    assert R.is_success()


def test_a_coverage_shard_still_clears_a_failure_that_passed_on_retry():
    child = _child("test_foo", Result.Status.FAIL, Result.Label.OK_ON_RETRY)
    R = _job_status([child], has_error=False, is_llvm_coverage=True)
    assert R.status == Result.Status.OK
    assert child.status == Result.Status.OK
    assert not child.has_label(Result.Label.OK_ON_RETRY)


def test_a_coverage_shard_still_reports_a_real_test_failure():
    R = _job_status(
        [_child("test_foo", Result.Status.FAIL)], has_error=False, is_llvm_coverage=True
    )
    assert R.status == Result.Status.FAIL
    assert "failed during LLVM coverage run" in R.info


def test_the_leaf_row_is_not_labelled_infra(tmp_path):
    """The second place the error could be erased: `main` clears `has_error` when every non-OK
    row carries `INFRA`. The leaf row deliberately does not, so a shard whose only problem is an
    exhausted budget still reports it."""
    tree = _leaf_tree(tmp_path, docker=_OWN_LIMIT_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    (row,) = leaf_oom_results(cgroup_root=tree)
    assert not row.has_label(Result.Label.INFRA)
    assert _job_status([row], has_error=True, is_llvm_coverage=True).status == (
        Result.Status.ERROR
    )


def test_a_coverage_shard_reports_a_test_failure_and_an_exhausted_leaf():
    """Both signals at once: the failure wins the status, and the error is not lost either."""
    R = _job_status(
        [
            _child("test_foo", Result.Status.FAIL),
            _child(_LEAF_OOM_ROW, Result.Status.ERROR),
        ],
        has_error=True,
        is_llvm_coverage=True,
    )
    assert not R.is_success()
    assert R.status in (Result.Status.FAIL, Result.Status.ERROR)


# --- the leaf that breaches after the first scan -------------------------------------------
# The first scan runs before the coverage shards' `llvm-profdata merge`, the last `/init` work.
# A merge the cap kills only prints, so without a second scan that shard finishes green minus its
# artifact - the r3 regression one phase later. These drive the production tail.


def _clean_coverage_job():
    """A coverage shard that ended green, as `main` has it once the first scan found nothing."""
    R = _job_status(
        [_child("test_foo", Result.Status.OK)], has_error=False, is_llvm_coverage=True
    )
    assert R.status == Result.Status.OK, "fixture precondition: the shard starts green"
    return R


def test_a_leaf_breaching_after_the_first_scan_still_reports(tmp_path):
    """The bug: `/init` is killed while merging coverage, after the only scan. The tree is
    mutated between the two calls, so this fails if the second scan is deleted or does not
    escalate."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    first, _ = leaf_oom_report(_REQUIRED, _CLEAN_DMESG, cgroup_root=tree)
    assert first == [], "fixture precondition: the first scan sees a clean tree"
    R = _clean_coverage_job()

    (tree / "init" / "memory.events.local").write_text(_OWN_LIMIT_OOM, encoding="utf-8")
    rows = report_late_leaf_ooms(
        R,
        _REQUIRED,
        _CLEAN_DMESG,
        {r.name for r in first},
        lost_coverage_artifact=True,
        cgroup_root=tree,
    )

    assert [r.name for r in rows] == [DIND_LEAF_MEANINGS["init"]]
    assert R.status == Result.Status.ERROR, R.info
    assert not R.is_success()
    assert DIND_LEAF_MEANINGS["init"] in [r.name for r in R.results]
    assert "coverage artifact" in R.info, "the ERROR does not name the observable consequence"


def test_a_leaf_breached_at_both_scans_yields_one_row(tmp_path):
    """The counters are cumulative, so the second scan sees the same breach again. One breach,
    one row: this reddens if the dedupe is dropped."""
    tree = _leaf_tree(tmp_path, docker=_OWN_LIMIT_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    first, _ = leaf_oom_report(_REQUIRED, _COLLECTIVE_BREACH, cgroup_root=tree)
    assert [r.name for r in first] == [_LEAF_OOM_ROW]
    # `main`'s state after the first scan: the row is a child and the job is already ERROR.
    R = _job_status(
        [_child("test_foo", Result.Status.OK), first[0]],
        has_error=True,
        is_llvm_coverage=True,
    )

    rows = report_late_leaf_ooms(
        R, _REQUIRED, _COLLECTIVE_BREACH, {r.name for r in first}, cgroup_root=tree
    )

    assert rows == [], "the same breach was reported twice"
    assert [r.name for r in R.results].count(_LEAF_OOM_ROW) == 1
    assert R.status == Result.Status.ERROR, "the first scan's error must survive"


def test_a_clean_run_stays_green_through_the_second_scan(tmp_path):
    """The scan itself must not turn every coverage shard red."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    R = _clean_coverage_job()
    assert (
        report_late_leaf_ooms(R, _REQUIRED, _CLEAN_DMESG, set(), cgroup_root=tree) == []
    )
    assert R.status == Result.Status.OK
    assert R.is_success()
    assert R.results == [r for r in R.results if r.name == "test_foo"]


def test_a_failed_merge_alone_is_not_a_job_error(tmp_path):
    """A merge that failed on its own is pre-existing behavior: only a leaf breach escalates."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    R = _clean_coverage_job()
    assert (
        report_late_leaf_ooms(
            R,
            _REQUIRED,
            _CLEAN_DMESG,
            set(),
            lost_coverage_artifact=True,
            cgroup_root=tree,
        )
        == []
    )
    assert R.status == Result.Status.OK
    assert "coverage artifact" not in R.info


def test_the_second_scan_is_skipped_without_containment(tmp_path):
    """Without the request these paths are the HOST's cgroups, same contract as the first scan."""
    tree = _leaf_tree(
        tmp_path, docker=_OWN_LIMIT_OOM, init=_OWN_LIMIT_OOM, dockerd=_OWN_LIMIT_OOM
    )
    R = _clean_coverage_job()
    assert report_late_leaf_ooms(R, {}, _COLLECTIVE_BREACH, set(), cgroup_root=tree) == []
    assert R.status == Result.Status.OK


def test_main_scans_again_after_the_last_init_work():
    """`main` must run the second scan, and run it AFTER the coverage merge - the `/init` work
    that made a second scan necessary. Deleting the call, or hoisting it above the merge, reddens
    this; nothing else in this file reaches the production tail."""
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    tree = ast.parse(source)
    main = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    calls = {}
    for node in ast.walk(main):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in ("report_late_leaf_ooms", "merge_profraw_files")
        ):
            calls.setdefault(node.func.id, []).append(node)
    late = calls.get("report_late_leaf_ooms", [])
    assert len(late) == 1, f"expected exactly one late scan, found {len(late)}"
    merge = calls.get("merge_profraw_files", [])
    assert len(merge) == 1, f"expected exactly one merge call, found {len(merge)}"
    assert late[0].lineno > merge[0].lineno, "the late scan runs before the coverage merge"

    args = [ast.unparse(a) for a in late[0].args + [k.value for k in late[0].keywords]]
    assert "os.environ" in args, "the containment request is not read from the environment"
    assert (
        "reported_leaf_ooms" in args
    ), "the first scan's rows are not passed, so a breach would be reported twice"

    # The scan must precede the completion that reports this run, and nothing may complete the
    # job between them: `complete_job` exits, so a status set afterwards is never reported.
    # `main` has several early-exit completions, hence the last one rather than the first.
    completions = [
        n.lineno
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "complete_job"
    ]
    assert completions, "main never completes the job"
    assert max(completions) > late[0].lineno, (
        "the late scan runs after the job is completed, so its ERROR is never reported"
    )
    assert not [
        c for c in completions if merge[0].lineno < c < late[0].lineno
    ], "the job is completed between the coverage merge and the late scan"


# --- the late scan owns the failure summary on the runs it fires ---------------------------
# `Result._add_job_summary_to_info` writes `Failures: N/M` only while `info` is empty, and
# `complete_job` calls it after this scan. Writing `info` here therefore deletes the failure
# count from the report and from the CIDB `test_context_raw` unless the scan emits it itself,
# which is what `report_rabbitmq_recreations` already does for the same reason.


def test_a_late_breach_keeps_the_jobs_failure_count(tmp_path):
    """Lost on exactly the runs that need it most: a red shard that also breached its budget.

    An ordinary shard, not a coverage one: test failures alone leave `has_error` false, so
    nothing has written `info` and the count is still `complete_job`'s to fill in. On a coverage
    shard `finalize_llvm_coverage_status` writes `info` first and the suppression is master's.
    """
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    R = _job_status(
        [_child("test_foo", Result.Status.OK), _child("test_bar", Result.Status.FAIL)],
        has_error=False,
        is_llvm_coverage=False,
    )
    assert R.info == "", "fixture precondition: nothing has written info yet"

    (tree / "init" / "memory.events.local").write_text(_OWN_LIMIT_OOM, encoding="utf-8")
    assert report_late_leaf_ooms(R, _REQUIRED, _CLEAN_DMESG, set(), cgroup_root=tree)

    R._add_job_summary_to_info()  # what complete_job does next
    assert "Failures: 2/3" in R.info, R.info
    assert R.info.count("Failures:") == 1, R.info


def test_the_summary_counts_the_late_row_itself(tmp_path):
    """Emitted after the rows are appended, so the breach is inside the count rather than
    reported beside a total that predates it."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    R = _clean_coverage_job()

    (tree / "init" / "memory.events.local").write_text(_OWN_LIMIT_OOM, encoding="utf-8")
    report_late_leaf_ooms(R, _REQUIRED, _CLEAN_DMESG, set(), cgroup_root=tree)

    assert "Failures: 1/2" in R.info, R.info


def test_an_earlier_writers_info_is_not_double_summarized(tmp_path):
    """`main` writes `info` itself on the host-OOM and session-error paths, where the summary is
    already suppressed by design, so the scan appends rather than adding a second one."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    R = _clean_coverage_job()
    R.set_info("Session-level error from another writer")

    (tree / "init" / "memory.events.local").write_text(_OWN_LIMIT_OOM, encoding="utf-8")
    report_late_leaf_ooms(R, _REQUIRED, _CLEAN_DMESG, set(), cgroup_root=tree)

    assert "Session-level error from another writer" in R.info
    assert "Failures:" not in R.info, R.info


def test_a_clean_run_leaves_the_summary_to_complete_job(tmp_path):
    """The scan writes nothing when it finds nothing, so `complete_job` still fills it in."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    R = _clean_coverage_job()

    assert report_late_leaf_ooms(R, _REQUIRED, _CLEAN_DMESG, set(), cgroup_root=tree) == []
    assert R.info == ""
    R._add_job_summary_to_info()
    assert "Failures: 0/1" in R.info


def test_the_count_survives_both_reporters_firing_together(tmp_path, monkeypatch):
    """A run that trips the late scan AND logs a RabbitMQ recreation. Its sibling reporter emits
    the summary only while `info` is empty, so with both firing the count was lost in the shipped
    order, and published stale (counting neither the leaf row nor its own effect) if swapped."""
    import ci.jobs.integration_test_job as job

    tree = _leaf_tree(tmp_path / "cg", docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    logs = tmp_path / "logs"
    logs.mkdir()
    monkeypatch.setattr(job, "temp_path", str(logs))
    (logs / "rabbit.log").write_text("fake broker log\n", encoding="utf-8")
    (logs / "pytest_parallel-gw0.log").write_text(
        f"Warning : {job.RABBITMQ_RECREATE_TOKEN} attempt=1 snapshot=rabbit.log"
        " RabbitMQ did not start in 120 seconds, recreating the container\n",
        encoding="utf-8",
    )
    R = _job_status(
        [_child("test_foo", Result.Status.OK), _child("test_bar", Result.Status.FAIL)],
        has_error=False,
        is_llvm_coverage=False,
    )
    assert R.info == "", "fixture precondition: nothing has written info yet"

    (tree / "init" / "memory.events.local").write_text(_OWN_LIMIT_OOM, encoding="utf-8")
    assert report_late_leaf_ooms(R, _REQUIRED, _CLEAN_DMESG, set(), cgroup_root=tree)
    assert job.report_rabbitmq_recreations(R) == 1

    R._add_job_summary_to_info()
    assert "Failures: 2/3" in R.info, R.info
    assert R.info.count("Failures:") == 1, R.info
    assert "recreation was attempted 1 time(s)" in R.info


# --- docker_in_docker.sh: the containment ladder, run against a fake cgroup tree -----------
# The block holds the riskiest logic in the change (five validators, the namespace guard whose
# failure mode is writing to the HOST root, and the EBUSY retry), and none of it is reachable
# from the assertions above: an inert stub would pass every one of them. Extracted verbatim
# between markers, following ci/tests/test_logs_cluster_probe_loop.py.

_VARS = [
    "CI_DIND_JOB_MEM",
    "CI_DIND_ROOT_RESERVE",
    "CI_DIND_INIT_RESERVE",
    "CI_DIND_DAEMON_RESERVE",
    "CI_DIND_NESTED_BUDGET",
    "CI_DIND_INIT_LIMIT",
]


def _extract_containment() -> str:
    """The containment block, verbatim, from between the BEGIN/END markers."""
    text = open(_DIND_SCRIPT, encoding="utf-8").read()
    m = re.search(
        r"# BEGIN: cgroup containment\n(.*?)\n\s*# END: cgroup containment",
        text,
        re.DOTALL,
    )
    assert m, "BEGIN/END cgroup-containment markers not found in docker_in_docker.sh"
    return textwrap.dedent(m.group(1))


_FAKE_CONTROLLERS = "memory pids"
# Seeded into the root `cgroup.procs`, so the migration the block performs before delegating is
# observable: delegation fails EBUSY while the root holds a process, and a block that skipped the
# move would leave these here.
_ROOT_PIDS = "4242\n7777\n"


def _fake_cgroup(
    tmp_path,
    own_cgroup="/",
    job_mem=2 * GIB,
    controllers=_FAKE_CONTROLLERS,
    cgroup_version=2,
    readonly_swap_files=False,
):
    """A cgroup-shaped tree plus the `/proc/self/cgroup` line the block reads.

    `cgroup_version=1` builds the v1 shape: the memory controller on its own mount, no
    `cgroup.controllers`/`cgroup.subtree_control`, and `memory.limit_in_bytes` as the limit file.

    `readonly_swap_files` makes only the swap control unwritable, which is how a kernel without
    swap accounting looks to the script: the memory cap lands and the swap cap cannot. A directory
    is used rather than a mode, because the block runs as root in CI and root ignores modes.
    """
    cg = tmp_path / "cgroup"
    cg.mkdir()
    proc = tmp_path / "proc_self_cgroup"
    if cgroup_version == 1:
        mem = cg / "memory"
        mem.mkdir()
        (mem / "memory.limit_in_bytes").write_text(f"{job_mem}\n", encoding="utf-8")
        (mem / "cgroup.procs").write_text(_ROOT_PIDS, encoding="utf-8")
        if readonly_swap_files:
            for leaf in DIND_LEAF_MEANINGS:
                (mem / leaf / "memory.memsw.limit_in_bytes").mkdir(parents=True)
        # A real v1 line carries every controller on its own numbered row, and `memory` may be
        # co-mounted; the block has to pick that row rather than the first or the last.
        proc.write_text(
            f"13:pids:/\n5:memory:{own_cgroup}\n2:cpu,cpuacct:/\n1:name=systemd:/\n",
            encoding="utf-8",
        )
        return cg, proc
    if controllers is not None:
        (cg / "cgroup.controllers").write_text(controllers, encoding="utf-8")
    (cg / "memory.max").write_text(f"{job_mem}\n", encoding="utf-8")
    (cg / "cgroup.procs").write_text(_ROOT_PIDS, encoding="utf-8")
    (cg / "cgroup.subtree_control").write_text("", encoding="utf-8")
    proc.write_text(f"0::{own_cgroup}\n", encoding="utf-8")
    if readonly_swap_files:
        for leaf in DIND_LEAF_MEANINGS:
            (cg / leaf / "memory.swap.max").mkdir(parents=True)
    return cg, proc


def _is_byte_count(value) -> bool:
    """What the script's own validator accepts: a non-empty run of digits."""
    return bool(value) and value.isdigit()


def _run_containment(tmp_path, env_overrides=None, **fake_kwargs):
    """Run the extracted block against a fake tree. Returns (rc, stdout+stderr, cg_path)."""
    cg, proc = _fake_cgroup(tmp_path, **fake_kwargs)
    job_mem = fake_kwargs.get("job_mem", 2 * GIB)
    env = dict(os.environ)
    env.update(
        {
            "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
            "CI_DIND_CGROUP_ROOT": str(cg),
            "CI_DIND_PROC_CGROUP": str(proc),
            "CI_DIND_JOB_MEM": str(job_mem),
            "CI_DIND_ROOT_RESERVE": str(256 * 1024**2),
            "CI_DIND_INIT_RESERVE": str(512 * 1024**2),
            # Derived the way `job_configs.py` derives it, so an override of any reserve keeps a
            # limit the script will accept and the tests exercise the production relationship.
            "CI_DIND_INIT_LIMIT": str(job_mem - 768 * 1024**2),
            "CI_DIND_DAEMON_RESERVE": str(512 * 1024**2),
            "CI_DIND_NESTED_BUDGET": str(job_mem - 1280 * 1024**2),
        }
    )
    for k, v in (env_overrides or {}).items():
        if v is None:
            env.pop(k, None)
        else:
            env[k] = v
    # An override that raises a reserve can leave the default limit below it; follow it up rather
    # than making every caller restate the limit. Skipped unless both values are byte counts: a
    # test that overrides one with a malformed value is asserting the refusal itself.
    limit, reserve = env.get("CI_DIND_INIT_LIMIT"), env.get("CI_DIND_INIT_RESERVE")
    caller_pinned_limit = "CI_DIND_INIT_LIMIT" in (env_overrides or {})
    if not caller_pinned_limit and _is_byte_count(limit) and _is_byte_count(reserve):
        env["CI_DIND_INIT_LIMIT"] = str(max(int(limit), int(reserve)))
    script = tmp_path / "block.sh"
    script.write_text(
        "set -u\n"
        # `$$` is the shell's own pid; the block moves it into the dockerd leaf, so print it
        # first to let the test assert the move actually happened.
        "echo SHELL_PID=$$\n"
        + _extract_containment()
        + "\necho REACHED_DOCKERD_START\n",
        encoding="utf-8",
    )
    p = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, timeout=120
    )
    return p.returncode, p.stdout + p.stderr, cg


def test_containment_happy_path_caps_every_leaf(tmp_path):
    rc, out, cg = _run_containment(tmp_path)
    assert rc == 0, out
    assert "containment active" in out
    for leaf in DIND_LEAF_MEANINGS:
        assert (cg / leaf / "memory.max").exists(), f"{leaf} was not capped: {out}"
    # `/init` is capped at its own limit, not at its share of the budget: a burst is one test's
    # host-side client fan-out, so pinning the share here is what killed shards 1/6 and 2/6.
    assert (cg / "init" / "memory.max").read_text().strip() == str(
        2 * GIB - 768 * 1024**2
    )
    assert (cg / "dockerd" / "memory.max").read_text().strip() == str(512 * 1024**2)
    assert (cg / "docker" / "memory.max").read_text().strip() == str(
        2 * GIB - 1280 * 1024**2
    )


def test_containment_happy_path_delegates_and_moves_the_processes(tmp_path):
    """The caps above are unenforceable without these two writes, and neither is visible in the
    leaf `memory.max` bodies - measured: with delegation and migration turned into no-ops the
    rest of this file still passed in full. So assert them directly.

    Without delegation the controllers are not available in the leaves at all; without the shell
    move the daemon shares `/docker`'s scope and is a legal OOM victim, which the negative
    control in the PR's evidence shows destroys the shard.
    """
    rc, out, cg = _run_containment(tmp_path)
    assert rc == 0, out

    # Delegation: every controller the root offers must be enabled for the children, in the
    # `+name` form the block writes. Derived from the fixture's own list, not a literal.
    enabled = (cg / "cgroup.subtree_control").read_text().split()
    assert enabled == [f"+{c}" for c in _FAKE_CONTROLLERS.split()], out

    # Migration: delegation fails EBUSY while the root holds a process, so the seeded pids must
    # have been moved into `/init`.
    assert (cg / "init" / "cgroup.procs").read_text().split() == _ROOT_PIDS.split(), out

    # The daemon runs from its own leaf: the block writes `$$`, which this harness prints.
    shell_pid = re.search(r"SHELL_PID=(\d+)", out).group(1)
    assert (cg / "dockerd" / "cgroup.procs").read_text().split() == [shell_pid], out


def test_containment_runs_before_the_daemon_is_started():
    """dockerd recreates `/docker` on startup, and the cap is measured to survive that only when
    written first. The harness above runs the block in isolation and so cannot check this;
    asserted against the script itself, where moving the block below the daemon would redden it."""
    text = open(_DIND_SCRIPT, encoding="utf-8").read()
    end_of_containment = text.index("# END: cgroup containment")
    starts_daemon = re.search(r"^\s*setsid dockerd ", text, re.MULTILINE)
    assert starts_daemon, "no `setsid dockerd` line found in docker_in_docker.sh"
    assert end_of_containment < starts_daemon.start()


@pytest.mark.parametrize("var", _VARS)
@pytest.mark.parametrize("bad", ["", "not-a-number", "12g", "-1"], ids=["empty", "text", "suffix", "negative"])
def test_containment_refuses_a_malformed_byte_count(tmp_path, var, bad):
    rc, out, cg = _run_containment(tmp_path, env_overrides={var: bad})
    assert rc == 3, out
    assert var in out
    assert not (cg / "init").exists(), "refused, yet it created leaves"


@pytest.mark.parametrize("var", [v for v in _VARS if v != "CI_DIND_JOB_MEM"])
def test_containment_refuses_a_value_that_would_overflow_its_own_sum(tmp_path, var):
    """Every value below is summed in Bash's signed 64-bit arithmetic, so one near the top of
    that range wraps negative and satisfies the `<= job limit` checks. Measured before the
    per-value bound: an 8 GiB job started a daemon with a 9223372036854775807 byte leaf cap.
    """
    rc, out, cg = _run_containment(
        tmp_path, env_overrides={var: str(2**63 - 1)}, job_mem=8 * GIB
    )
    assert rc == 3, out
    assert var in out
    assert not (cg / "init").exists(), "refused, yet it created leaves"


def test_containment_refuses_when_the_namespace_is_not_private(tmp_path):
    """The catastrophic path: unqualified paths under `--cgroupns=host` are the HOST's root, so
    the block must refuse before writing anything at all."""
    rc, out, cg = _run_containment(
        tmp_path, own_cgroup="/system.slice/docker-abc123.scope"
    )
    assert rc == 3, out
    assert "not private" in out
    assert (cg / "cgroup.subtree_control").read_text() == ""
    assert not (cg / "init").exists()
    assert sorted(p.name for p in cg.iterdir()) == [
        "cgroup.controllers",
        "cgroup.procs",
        "cgroup.subtree_control",
        "memory.max",
    ]


def test_containment_refuses_when_the_job_limit_did_not_apply(tmp_path):
    rc, out, cg = _run_containment(
        tmp_path, env_overrides={"CI_DIND_JOB_MEM": str(4 * GIB)}
    )
    assert rc == 3, out
    assert "root memory.max" in out
    assert not (cg / "init").exists()


def test_containment_refuses_when_neither_hierarchy_is_present(tmp_path):
    """No `cgroup.controllers` and no `memory/` means no way to cap anything, so refuse."""
    rc, out, cg = _run_containment(tmp_path, controllers=None)
    assert rc == 3, out
    assert "cgroup v2" in out and "cgroup v1" in out, out
    assert not (cg / "init").exists(), "refused, yet it created leaves"


# --- cgroup v1 ---------------------------------------------------------------------------
# The CI runners boot `systemd.unified_cgroup_hierarchy=0`, so production is v1 while every
# other arm of this file (and the fleet's own host) is v2. Measured on a v1 kernel: the outer
# `--memory` shows up as `memory.limit_in_bytes`, `--cgroupns=private` namespaces the v1
# hierarchy too, and the `/docker` cap does bind containers the daemon creates with no limit
# of their own.


def test_v1_caps_every_leaf_under_the_memory_controller(tmp_path):
    rc, out, cg = _run_containment(tmp_path, cgroup_version=1)
    assert rc == 0, out
    assert "containment active (cgroup v1)" in out, out
    mem = cg / "memory"
    assert (mem / "init" / "memory.limit_in_bytes").read_text().strip() == str(
        2 * GIB - 768 * 1024**2
    )
    assert (mem / "dockerd" / "memory.limit_in_bytes").read_text().strip() == str(
        512 * 1024**2
    )
    assert (mem / "docker" / "memory.limit_in_bytes").read_text().strip() == str(
        2 * GIB - 1280 * 1024**2
    )
    # The v2 files must not appear, or a v2 reader would find an uncapped-looking tree.
    assert not (mem / "docker" / "memory.max").exists()


def test_v1_moves_the_harness_into_the_capped_init_leaf(tmp_path):
    """v2 migrates the root processes as a side effect of delegation; v1 has to do it explicitly.

    Without it pytest and its xdist workers keep running in the uncapped root, so a harness
    overrun hits the job's own limit - the outer OOM this change exists to prevent - while the
    8 GiB `/init` leaf sits empty.
    """
    rc, out, cg = _run_containment(tmp_path, cgroup_version=1)
    assert rc == 0, out
    mem = cg / "memory"
    moved = (mem / "init" / "cgroup.procs").read_text().split()
    assert sorted(moved) == sorted(_ROOT_PIDS.split()), out


def test_v1_refuses_if_the_harness_cannot_be_moved(tmp_path):
    """Leaving processes in the root silently reinstates the outer-OOM failure mode, so it must
    refuse rather than run with an empty `/init`."""
    cg, proc = _fake_cgroup(tmp_path, cgroup_version=1)
    # A directory where the file belongs: writes fail for root too, unlike a mode-based denial
    # (CI Tests runs as root, which ignores the mode bits).
    (cg / "memory" / "init" / "cgroup.procs").mkdir(parents=True)
    env = dict(os.environ)
    env.update(
        {
            "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
            "CI_DIND_CGROUP_ROOT": str(cg),
            "CI_DIND_PROC_CGROUP": str(proc),
            "CI_DIND_JOB_MEM": str(2 * GIB),
            "CI_DIND_ROOT_RESERVE": str(256 * 1024**2),
            "CI_DIND_INIT_RESERVE": str(512 * 1024**2),
            "CI_DIND_INIT_LIMIT": str(2 * GIB - 768 * 1024**2),
            "CI_DIND_DAEMON_RESERVE": str(512 * 1024**2),
            "CI_DIND_NESTED_BUDGET": str(2 * GIB - 1280 * 1024**2),
        }
    )
    script = tmp_path / "block.sh"
    script.write_text("set -u\n" + _extract_containment(), encoding="utf-8")
    p = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, timeout=120
    )
    out = p.stdout + p.stderr
    assert p.returncode == 3, out
    assert "left it empty" in out, out


def test_v1_also_limits_memory_plus_swap(tmp_path):
    """A cap on `memory.limit_in_bytes` alone lets a leaf exceed it by swapping, so v1 needs the
    memsw limit too, which counts memory and swap against one number."""
    rc, out, cg = _run_containment(tmp_path, cgroup_version=1)
    assert rc == 0, out
    mem = cg / "memory"
    assert (mem / "docker" / "memory.memsw.limit_in_bytes").read_text().strip() == str(
        2 * GIB - 1280 * 1024**2
    )
    assert (mem / "dockerd" / "memory.memsw.limit_in_bytes").read_text().strip() == str(
        512 * 1024**2
    )
    # `/init`'s memsw tracks its own limit, not its share of the budget. A memsw left at the share
    # re-imposes the wall the limit exists to lift, while `memory.limit_in_bytes` still reads as
    # raised, so no other assertion here would catch it.
    assert (mem / "init" / "memory.memsw.limit_in_bytes").read_text().strip() == str(
        2 * GIB - 768 * 1024**2
    )


def test_v2_also_limits_swap(tmp_path):
    """`memory.max` bounds resident pages only; v2's `memory.swap.max` is separate and defaults to
    unlimited, so without it a leaf can still exceed its advertised budget by swapping."""
    rc, out, cg = _run_containment(tmp_path)
    assert rc == 0, out
    for leaf in DIND_LEAF_MEANINGS:
        assert (cg / leaf / "memory.swap.max").read_text().strip() == "0", leaf


def test_v1_runs_the_daemon_from_its_own_leaf(tmp_path):
    """Same invariant as v2: dockerd must not share `/docker`'s scope, or the kernel may pick it
    as the victim of an overrun by the containers."""
    rc, out, cg = _run_containment(tmp_path, cgroup_version=1)
    assert rc == 0, out
    shell_pid = re.search(r"SHELL_PID=(\d+)", out).group(1)
    moved = (cg / "memory" / "dockerd" / "cgroup.procs").read_text().split()
    assert shell_pid in moved, out


def test_v1_skips_the_v2_only_delegation(tmp_path):
    """v1 has no `cgroup.subtree_control`; attempting the v2 dance would refuse on a real host."""
    rc, out, cg = _run_containment(tmp_path, cgroup_version=1)
    assert rc == 0, out
    assert "subtree_control" not in out, out
    assert not (cg / "memory" / "cgroup.subtree_control").exists()


# Real lines from a cgroup-v1 kernel (6.8, `systemd.unified_cgroup_hierarchy=0`). The v1 paths
# carry the outer container id as a prefix rather than starting at the leaf, which is why the
# detector compares only the last component.
_V1_COLLECTIVE_BREACH = (
    "oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=48f7e9846f51,mems_allowed=0,"
    "oom_memcg=/docker/c1f00d4ab6d6/docker,"
    "task_memcg=/docker/c1f00d4ab6d6/docker/48f7e9846f51,task=python3,pid=12264,uid=0\n"
).encode()
_V1_OWN_MEM_LIMIT = (
    "oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),cpuset=47c083eaaf7b,mems_allowed=0,"
    "oom_memcg=/docker/47c083eaaf7b/docker/childA,"
    "task_memcg=/docker/47c083eaaf7b/docker/childA,task=python3,pid=5855,uid=0\n"
).encode()


def test_v1_dmesg_reports_a_collective_breach_of_the_container_budget():
    """On v1 the dmesg scan is the ONLY detector: no v1 counter says "this leaf's limit was
    breached" (`oom_kill` is charged to the killed task's cgroup, and `failcnt` also counts hits
    that reclaim satisfied). So this pins the one signal v1 has."""
    assert dind_leaf_oom_in_dmesg(_V1_COLLECTIVE_BREACH) == {"docker"}


def test_v1_dmesg_ignores_a_container_hitting_its_own_mem_limit():
    """The other half: a module's container reaching the `mem_limit` it asked for is tolerated
    behaviour, and on v1 it differs from the line above only in `oom_memcg=`."""
    assert dind_leaf_oom_in_dmesg(_V1_OWN_MEM_LIMIT) == set()


def test_v1_reads_the_memory_row_of_proc_self_cgroup(tmp_path):
    """The v1 line has one row per controller, so a namespace check that read the first or last
    row would pass on a host-namespaced container and write into the HOST's cgroups."""
    rc, out, _ = _run_containment(tmp_path, cgroup_version=1, own_cgroup="/docker/abc123")
    assert rc == 3, out
    assert "namespace is not private" in out, out
    assert "/docker/abc123" in out, out


def test_v1_refuses_when_the_outer_limit_did_not_apply(tmp_path):
    """`memory.limit_in_bytes` not matching means the job's `--memory` never took effect, so the
    leaves would be sized against a limit that is not there."""
    cg, proc = _fake_cgroup(tmp_path, job_mem=2 * GIB, cgroup_version=1)
    (cg / "memory" / "memory.limit_in_bytes").write_text(
        f"{9223372036854771712}\n", encoding="utf-8"
    )
    env = dict(os.environ)
    env.update(
        {
            "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
            "CI_DIND_CGROUP_ROOT": str(cg),
            "CI_DIND_PROC_CGROUP": str(proc),
            "CI_DIND_JOB_MEM": str(2 * GIB),
            "CI_DIND_ROOT_RESERVE": str(256 * 1024**2),
            "CI_DIND_INIT_RESERVE": str(512 * 1024**2),
            "CI_DIND_INIT_LIMIT": str(2 * GIB - 768 * 1024**2),
            "CI_DIND_DAEMON_RESERVE": str(512 * 1024**2),
            "CI_DIND_NESTED_BUDGET": str(2 * GIB - 1280 * 1024**2),
        }
    )
    script = tmp_path / "block.sh"
    script.write_text("set -u\n" + _extract_containment(), encoding="utf-8")
    p = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, timeout=120
    )
    assert p.returncode == 3, p.stdout + p.stderr
    assert "memory.limit_in_bytes is" in p.stdout + p.stderr


def test_containment_refuses_a_budget_that_leaves_containers_nothing(tmp_path):
    rc, out, cg = _run_containment(
        tmp_path, env_overrides={"CI_DIND_NESTED_BUDGET": "0"}
    )
    assert rc == 3, out
    assert "nothing" in out


def test_containment_refuses_reserves_above_the_job_limit(tmp_path):
    rc, out, _ = _run_containment(
        tmp_path, env_overrides={"CI_DIND_INIT_RESERVE": str(8 * GIB)}
    )
    assert rc == 3, out
    assert "above the job limit" in out


def test_containment_retries_delegation_while_a_pid_is_still_in_the_root(tmp_path):
    """Delegation fails EBUSY while the cgroup holds a process, and the caller keeps forking,
    so a single pass loses the delegation silently. The fake `cgroup.subtree_control` rejects
    the first write and accepts the next, which only a retrying block survives."""
    cg, proc = _fake_cgroup(tmp_path)
    # A directory cannot be written to, so the first `>` fails; the block's retry replaces it.
    (cg / "cgroup.subtree_control").unlink()
    (cg / "cgroup.subtree_control").mkdir()
    env = dict(os.environ)
    env.update(
        {
            "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
            "CI_DIND_CGROUP_ROOT": str(cg),
            "CI_DIND_PROC_CGROUP": str(proc),
            "CI_DIND_JOB_MEM": str(2 * GIB),
            "CI_DIND_ROOT_RESERVE": str(256 * 1024**2),
            "CI_DIND_INIT_RESERVE": str(512 * 1024**2),
            "CI_DIND_INIT_LIMIT": str(2 * GIB - 768 * 1024**2),
            "CI_DIND_DAEMON_RESERVE": str(512 * 1024**2),
            "CI_DIND_NESTED_BUDGET": str(2 * GIB - 1280 * 1024**2),
        }
    )
    unblock = tmp_path / "unblock.sh"
    unblock.write_text(
        f'sleep 2; rmdir "{cg}/cgroup.subtree_control"; '
        f': > "{cg}/cgroup.subtree_control"\n',
        encoding="utf-8",
    )
    script = tmp_path / "block.sh"
    script.write_text(
        f'set -u\nbash "{unblock}" &\n' + _extract_containment() + "\n", encoding="utf-8"
    )
    p = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, timeout=180
    )
    assert p.returncode == 0, p.stdout + p.stderr
    assert "containment active" in p.stdout + p.stderr


def test_containment_refuses_when_delegation_never_succeeds(tmp_path):
    cg, proc = _fake_cgroup(tmp_path)
    (cg / "cgroup.subtree_control").unlink()
    (cg / "cgroup.subtree_control").mkdir()
    env = dict(os.environ)
    env.update(
        {
            "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
            "CI_DIND_CGROUP_ROOT": str(cg),
            "CI_DIND_PROC_CGROUP": str(proc),
            "CI_DIND_JOB_MEM": str(2 * GIB),
            "CI_DIND_ROOT_RESERVE": str(256 * 1024**2),
            "CI_DIND_INIT_RESERVE": str(512 * 1024**2),
            "CI_DIND_INIT_LIMIT": str(2 * GIB - 768 * 1024**2),
            "CI_DIND_DAEMON_RESERVE": str(512 * 1024**2),
            "CI_DIND_NESTED_BUDGET": str(2 * GIB - 1280 * 1024**2),
        }
    )
    script = tmp_path / "block.sh"
    script.write_text("set -u\n" + _extract_containment() + "\n", encoding="utf-8")
    p = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, timeout=300
    )
    assert p.returncode == 3, p.stdout + p.stderr
    assert "kept failing" in p.stdout + p.stderr


def test_permissive_path_touches_nothing(tmp_path):
    """Without the request the block is skipped entirely, so jobs that do not opt in keep
    today's behavior."""
    cg, proc = _fake_cgroup(tmp_path)
    before = sorted(p.name for p in cg.iterdir())
    env = dict(os.environ)
    env.pop("CI_DIND_REQUIRE_CGROUP_CONTAINMENT", None)
    env.update({"CI_DIND_CGROUP_ROOT": str(cg), "CI_DIND_PROC_CGROUP": str(proc)})
    for var in _VARS:
        env.pop(var, None)
    script = tmp_path / "block.sh"
    script.write_text("set -u\n" + _extract_containment() + "\n", encoding="utf-8")
    p = subprocess.run(
        ["bash", str(script)], env=env, capture_output=True, text=True, timeout=60
    )
    assert p.returncode == 0, p.stdout + p.stderr
    assert sorted(q.name for q in cg.iterdir()) == before
    assert (cg / "cgroup.subtree_control").read_text() == ""
    # Nothing was delegated and nothing was migrated: the seeded pids are still in the root.
    assert (cg / "cgroup.procs").read_text() == _ROOT_PIDS


# --- the job config carries the topology the script requires -----------------------------


def test_job_requests_a_private_cgroup_namespace():
    """Explicitly, not by relying on the daemon default: `--default-cgroupns-mode` can be
    `host`, which would restore the leak with the config still looking correct."""
    run_in_docker = common_integration_test_job_config.run_in_docker
    assert "--cgroupns=private" in run_in_docker
    assert "--cgroupns=host" not in run_in_docker


def test_job_passes_the_budget_and_reserves_to_the_script():
    """All three reserves, not just the budget: the script validates that they are byte counts
    summing within the job limit, so an exported reserve that is well-formed but stale - an
    `/init` left at a value the harness is known to exhaust, say - passes every check the shell
    makes and reinstates the overrun with the configuration still looking right."""
    run_in_docker = common_integration_test_job_config.run_in_docker
    assert "--env=CI_DIND_REQUIRE_CGROUP_CONTAINMENT=1" in run_in_docker
    assert f"--env=CI_DIND_JOB_MEM={LIMITED_MEM}" in run_in_docker
    assert f"--env=CI_DIND_ROOT_RESERVE={INTEGRATION_DIND_ROOT_RESERVE}" in run_in_docker
    assert f"--env=CI_DIND_INIT_RESERVE={INTEGRATION_DIND_INIT_RESERVE}" in run_in_docker
    assert (
        f"--env=CI_DIND_DAEMON_RESERVE={INTEGRATION_DIND_DAEMON_RESERVE}"
        in run_in_docker
    )
    assert f"--env=CI_DIND_NESTED_BUDGET={INTEGRATION_NESTED_BUDGET}" in run_in_docker
    assert f"--memory={LIMITED_MEM}" in run_in_docker


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))


# --- leaf peak usage: the readout that makes the next reserve a measurement -----------------


def _peak_tree(tmp_path, peaks, filename="memory.peak"):
    for leaf, value in peaks.items():
        (tmp_path / leaf).mkdir(parents=True, exist_ok=True)
        (tmp_path / leaf / filename).write_text(f"{value}\n", encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize(
    "filename", ["memory.peak", "memory.max_usage_in_bytes"], ids=["v2", "v1"]
)
def test_leaf_peak_usage_reads_both_cgroup_versions(tmp_path, filename):
    """The CI carriers are v1 and the fixtures elsewhere in this file are v2, so a readout that
    knows only one name reports nothing on the hosts that matter."""
    _peak_tree(tmp_path, {"init": 3 * GIB, "docker": 9 * GIB}, filename=filename)
    assert leaf_peak_usage(cgroup_root=tmp_path) == {"init": 3 * GIB, "docker": 9 * GIB}


def test_leaf_peak_usage_tolerates_a_leaf_that_cannot_report(tmp_path):
    """A kernel without the file, or the permissive path that creates no leaves at all."""
    _peak_tree(tmp_path, {"init": GIB})
    (tmp_path / "dockerd").mkdir()
    (tmp_path / "docker").mkdir()
    (tmp_path / "docker" / "memory.peak").write_text("nonsense\n", encoding="utf-8")
    assert leaf_peak_usage(cgroup_root=tmp_path) == {"init": GIB}


def test_the_peak_readout_is_silent_without_required_containment(tmp_path, capsys):
    """Outside required containment these paths are the host's cgroups, so reporting them would
    describe the runner rather than the job."""
    _peak_tree(tmp_path, {"init": GIB})
    assert print_leaf_peak_usage({}, cgroup_root=tmp_path) == {}
    assert capsys.readouterr().out == ""


def test_the_peak_readout_flags_a_leaf_pinned_at_its_cap(tmp_path, capsys):
    """The distinction the sizing depends on: a peak below the cap is the workload's footprint,
    while a peak AT the cap is only a lower bound, because the kernel capped the growth."""
    _peak_tree(tmp_path, {"init": 8 * GIB, "docker": 10 * GIB})
    env = {
        "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
        "CI_DIND_INIT_RESERVE": str(8 * GIB),
        "CI_DIND_NESTED_BUDGET": str(40 * GIB),
    }
    assert print_leaf_peak_usage(env, cgroup_root=tmp_path) == {
        "init": 8 * GIB,
        "docker": 10 * GIB,
    }
    out = capsys.readouterr().out
    init_line = next(l for l in out.splitlines() if "/init" in l)
    docker_line = next(l for l in out.splitlines() if "/docker" in l)
    assert "AT CAP" in init_line, init_line
    assert "8.00 GiB of 8.00 GiB cap" in init_line, init_line
    assert "AT CAP" not in docker_line, docker_line
    assert "10.00 GiB of 40.00 GiB cap" in docker_line, docker_line


def test_the_peak_readout_measures_init_against_the_written_cap(tmp_path, capsys):
    """`/init`'s share and its cap differ, and the script writes the cap. Reporting the share
    would call an ordinary peak censored and hide the real headroom, which is the figure the
    sizing decision is made from."""
    _peak_tree(tmp_path, {"init": 16 * GIB})
    env = {
        "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
        "CI_DIND_INIT_RESERVE": str(16 * GIB),
        "CI_DIND_INIT_LIMIT": str(56 * GIB),
        "CI_DIND_NESTED_BUDGET": str(40 * GIB),
    }
    print_leaf_peak_usage(env, cgroup_root=tmp_path)
    init_line = next(l for l in capsys.readouterr().out.splitlines() if "/init" in l)
    assert "16.00 GiB of 56.00 GiB cap" in init_line, init_line
    assert "AT CAP" not in init_line, init_line


def test_the_peak_readout_runs_after_the_last_init_work():
    """It must not sit behind the `is_local_run` guard that gates the dmesg dump: the peaks come
    from this container's own cgroup, and a local run is exactly where an operator tunes them."""
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    call = source.index("print_leaf_peak_usage(os.environ)")
    merge = source.index("merge_profraw_files(llvm_profdata_cmd, job_params)")
    # The CALL, not the `def`: searching the bare name finds the definition far above.
    late_dmesg = source.index("late_breach = report_late_leaf_ooms(")
    assert merge < call < late_dmesg
    # Unguarded means the call's own statement is at `main`'s indentation, not nested inside the
    # `if not info.is_local_run:` block that follows it.
    line_start = source.rindex("\n", 0, call) + 1
    assert source[line_start:call] == "    ", repr(source[line_start:call])


def test_the_local_path_also_reports_a_leaf_oom():
    """A local run must not report a contained kill as success.

    The counters read this container's own cgroup, so only the host-wide dmesg scan needs a CI
    host. Asserted on the source because the surrounding block needs a full `main`: what makes
    the local path safe is that the report is not nested under the `is_local_run` guard, and that
    the guard covers only the dmesg dump and the artifact it attaches.
    """
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    report = source.index("leaf_results, attach_dmesg = leaf_oom_report(os.environ, ")
    line_start = source.rindex("\n", 0, report) + 1
    assert source[line_start:report] == "    ", repr(source[line_start:report])
    # The dmesg dump stays conditional, and `dmesg` is defined before it so the empty-bytes case
    # is what a local run passes to the report.
    dump = source.index('Shell.check("dmesg -T > ./ci/tmp/dmesg.log"')
    empty = source.index('    dmesg = b""')
    assert empty < dump < report
    assert "if not info.is_local_run:" in source[empty:dump]
    # The dmesg.log artifact is only attached on the path that produced it.
    attach = source.index('attached_files.append("./ci/tmp/dmesg.log")')
    assert source.rindex("if not info.is_local_run:", 0, attach) > report


def test_an_empty_dmesg_still_reports_a_breached_leaf_from_the_counters(tmp_path):
    """The local path's inputs, at the function the source test pins: no dmesg, counters only."""
    tree = _leaf_tree(tmp_path, init=_OWN_LIMIT_OOM, docker=_NO_OOM, dockerd=_NO_OOM)
    results, attach = leaf_oom_report(
        {"CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1"}, b"", cgroup_root=tree
    )
    assert [r.name for r in results] == [DIND_LEAF_MEANINGS["init"]]
    assert all(r.status == Result.Status.ERROR for r in results)
    assert attach is True


def test_the_leaf_root_follows_the_cgroup_hierarchy(tmp_path):
    """v1 mounts the memory controller on its own, so the script's leaves live under `memory/`.

    The production runners are v1, so a reader that assumes the v2 layout reports nothing exactly
    where the reserves are enforced.
    """
    v2 = tmp_path / "v2"
    (v2 / "init").mkdir(parents=True)
    (v2 / "cgroup.controllers").write_text("memory pids\n", encoding="utf-8")
    assert dind_leaf_root(v2) == v2

    v1 = tmp_path / "v1"
    (v1 / "memory" / "init").mkdir(parents=True)
    assert dind_leaf_root(v1) == v1 / "memory"


def test_leaf_peak_usage_reads_the_v1_leaves_the_script_creates(tmp_path):
    """End to end against the topology `docker_in_docker.sh` builds on v1: `<root>/memory/<leaf>`,
    reporting the memsw peak, which is the one that can reach a cap v1 applies to memory+swap."""
    mem = tmp_path / "memory"
    for leaf, peak in (("init", 6 * GIB), ("docker", 30 * GIB)):
        (mem / leaf).mkdir(parents=True)
        (mem / leaf / "memory.max_usage_in_bytes").write_text(
            f"{peak // 2}\n", encoding="utf-8"
        )
        (mem / leaf / "memory.memsw.max_usage_in_bytes").write_text(
            f"{peak}\n", encoding="utf-8"
        )
    assert leaf_peak_usage(cgroup_root=tmp_path) == {"init": 6 * GIB, "docker": 30 * GIB}


def test_the_late_scan_also_runs_on_a_local_run():
    """A `/init` kill during the coverage merge is the last thing that can happen, and the merge
    is not raised on failure, so a local run that skipped this scan would finish green."""
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    late = source.index("late_breach = report_late_leaf_ooms(")
    line_start = source.rindex("\n", 0, late) + 1
    assert source[line_start:late] == "    ", repr(source[line_start:late])
    empty = source.index('    late_dmesg = b""')
    assert empty < late
    # The artifact is still attached only on the path that produced it. `late_dmesg_dumped` is
    # that path exactly: it is set where the redirect succeeded, which a local run never reaches.
    attach = source.index("R.files.append(LATE_DMESG_LOG)", late)
    assert "late_dmesg_dumped" in source[late:attach]


def test_the_late_dmesg_is_only_read_when_its_re_dump_succeeded():
    """`Shell.check` is best-effort, and the first scan's file is still on disk: an unconditional
    reread dates the evidence to before the coverage merge, which is the only window this scan
    exists to cover. It also refills `late_dmesg`, suppressing the undetectable-leaf warning.

    Pinned on the job script because that is where the bug lives - the mechanism alone holds
    whatever the call site does with it (see the companion test below).
    """
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    reads = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.With)
        and "late_dmesg_file" in ast.unparse(n.items[0])
    ]
    assert len(reads) == 1, f"expected one late-dmesg read, found {len(reads)}"
    guards = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and "Shell.check" in ast.unparse(n.test)
        and "dmesg" in ast.unparse(n.test)
        and any(reads[0] is d for d in ast.walk(n))
    ]
    assert guards, "the late dmesg is read without checking that the re-dump succeeded"


def test_a_failed_re_dump_does_not_reread_the_first_scans_buffer(tmp_path):
    """Why the guard above is the right shape: `>` truncates when it OPENS the file, so a command
    that merely exits non-zero leaves it empty anyway - but a redirect that cannot open it at all
    leaves the previous dump readable, which is the case the return value discriminates.
    """
    log = tmp_path / "dmesg.log"

    def late_dmesg_for(command: str, target=None) -> bytes:
        log.write_text("STALE: from the first scan\n", encoding="utf-8")
        if Shell.check(f"{command} > {target or log}", verbose=False):
            return log.read_bytes()
        return b""

    assert late_dmesg_for("printf 'FRESH\\n'") == b"FRESH\n"
    # The command runs, so the redirect truncates: empty either way, but for the wrong reason.
    assert late_dmesg_for("sh -c 'exit 1'") == b""
    # The redirect itself fails, which is the case that leaves the stale buffer readable. A mode
    # bit would not do: `CI Tests` runs as root, which truncates a read-only file regardless.
    assert late_dmesg_for("printf 'FRESH\\n'", target=tmp_path / "gone" / "dmesg.log") == b""
    assert log.read_text(encoding="utf-8") == "STALE: from the first scan\n"


def test_the_late_re_dump_writes_a_different_file_from_the_first_scan():
    """Both scans redirect, and `>` truncates on open, so sharing a path lets the late dump empty
    an artifact the first scan already queued for upload - losing the evidence for the very
    host-OOM or early breach that attached it.

    Every redirect target in the script is collected, so adding a third dump over the shared
    path fails here too.
    """
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    first = source.index('Shell.check("dmesg -T > ./ci/tmp/dmesg.log"')
    late = source.index('Shell.check(f"dmesg -T > {LATE_DMESG_LOG}"', first)
    assert LATE_DMESG_LOG != "./ci/tmp/dmesg.log"
    # The first scan's file is what `attached_files` carries; the late one must not name it.
    assert '"./ci/tmp/dmesg.log"' not in source[late:]
    targets = re.findall(r"dmesg -T > ([^\"']+)", source)
    assert sorted(set(targets)) == sorted({"./ci/tmp/dmesg.log", "{LATE_DMESG_LOG}"}), targets


def test_a_failed_late_re_dump_leaves_the_attached_first_dump_intact(tmp_path):
    """The mechanism the test above pins: a re-dump over the shared path destroys the earlier
    evidence even when the command fails, because the truncation happens at open."""
    attached = tmp_path / "dmesg.log"
    late = tmp_path / "dmesg-after-merge.log"
    evidence = "Out of memory: Killed process 123\n"

    attached.write_text(evidence, encoding="utf-8")
    assert not Shell.check(f"sh -c 'exit 1' > {attached}", verbose=False)
    assert attached.read_text(encoding="utf-8") == "", "shared path: the artifact is destroyed"

    attached.write_text(evidence, encoding="utf-8")
    assert not Shell.check(f"sh -c 'exit 1' > {late}", verbose=False)
    assert attached.read_text(encoding="utf-8") == evidence, "separate path: artifact preserved"


def test_the_worker_count_is_read_after_param_injection():
    """`--param KEY=VALUE` writes `os.environ` inside `main`, so a budget captured at import is
    stale by the time workers are sized: on a 755 GiB host a `--param CI_DIND_NESTED_BUDGET=49GiB`
    run would plan 68 workers against a 49 GiB cap - the overcommit this change removes.

    Pinned as an ordering property on `main` rather than on a module global, because the defect
    is that the read happens too early, not what any one name holds.
    """
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    body = [ast.unparse(n) for n in main.body]
    param = next(i for i, s in enumerate(body) if s.startswith("if args.param:"))
    sizing = next(i for i, s in enumerate(body) if "planned_workers(" in s)
    assert param < sizing, "workers are sized before --param is applied"
    call = body[sizing]
    assert "nested_budget_gb()" in call, f"the budget is not re-read at the call site: {call}"


def test_no_module_global_caches_the_budget():
    """The other half: a module-level `mem_gb = nested_budget_gb()` is frozen at import, so
    reintroducing one restores the staleness even with the call site above ordered correctly."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    cached = [
        ast.unparse(n)
        for n in tree.body
        if isinstance(n, ast.Assign) and "nested_budget_gb(" in ast.unparse(n.value)
    ]
    assert cached == [], f"the budget is cached at import: {cached}"


def test_an_unreadable_late_dmesg_still_warns_about_an_undetectable_leaf(tmp_path):
    """The warning keys on an empty `late_dmesg`, so suppressing it needs the read to be skipped:
    a stale non-empty buffer would claim `/docker` was covered when nothing fresh was read."""
    # v1: no `memory.events.local`, so only `memory.oom_control` is present.
    tree = _leaf_tree(tmp_path, init=_NO_OOM, dockerd=_NO_OOM)
    (tree / "docker" / "memory.oom_control").write_text("oom_kill 0\n", encoding="utf-8")
    assert dind_unreportable_ooms(_REQUIRED, False, cgroup_root=tree) == [
        DIND_LEAF_MEANINGS["docker"],
        DIND_JOB_CGROUP_OOM,
    ]
    assert dind_unreportable_ooms(_REQUIRED, True, cgroup_root=tree) == []


def _swapful_meminfo(tmp_path, kb=4194304):
    path = tmp_path / "meminfo"
    path.write_text(f"MemTotal: 65851400 kB\nSwapTotal: {kb} kB\n", encoding="utf-8")
    return path


def test_a_leaf_that_cannot_be_swap_limited_refuses_when_the_host_has_swap(tmp_path):
    """The fail-open case: if the swap control cannot be written and swap exists, the leaf's real
    ceiling is higher than the budget it advertises, so starting the daemon would contain nothing.

    The write is made to fail by pointing the tree at a read-only leaf, which is what a kernel
    without swap accounting looks like from the script's side: the file is simply not writable.
    """
    rc, out, _ = _run_containment(
        tmp_path,
        env_overrides={"CI_DIND_PROC_MEMINFO": str(_swapful_meminfo(tmp_path))},
        readonly_swap_files=True,
    )
    assert rc == 3, out
    assert "capping swap" in out, out
    assert "[4194304] kB of swap" in out, out


def test_a_leaf_that_cannot_be_swap_limited_still_runs_on_a_swapless_host(tmp_path):
    """The other side of that boundary: with no swap the memory cap is already the whole limit, so
    an unwritable swap control must not stop the job."""
    rc, out, _ = _run_containment(
        tmp_path,
        env_overrides={"CI_DIND_PROC_MEMINFO": str(_swapful_meminfo(tmp_path, kb=0))},
        readonly_swap_files=True,
    )
    assert rc == 0, out
    assert "containment active" in out, out


@pytest.mark.parametrize(
    "meminfo",
    ["missing", "no-swaptotal-field", "malformed"],
)
def test_an_unreadable_swap_total_is_not_read_as_a_swapless_host(tmp_path, meminfo):
    """Only an OBSERVED zero excuses a failed swap write.

    Collapsing an unreadable or absent `SwapTotal` into zero would let every swap control fail and
    still start the daemon, which is the fail-open case the refusal exists to prevent.
    """
    path = tmp_path / "meminfo"
    if meminfo == "no-swaptotal-field":
        path.write_text("MemTotal: 65851400 kB\n", encoding="utf-8")
    elif meminfo == "malformed":
        path.write_text("not a meminfo file at all\n", encoding="utf-8")
    rc, out, _ = _run_containment(
        tmp_path,
        env_overrides={"CI_DIND_PROC_MEMINFO": str(path)},
        readonly_swap_files=True,
    )
    assert rc == 3, out
    assert "capping swap" in out, out
    assert "[unknown] kB of swap" in out, out


def _v1_leaf_counters(tmp_path, oom_kills):
    """A v1 tree with `memory.oom_control` per leaf, as the kernel presents it."""
    mem = tmp_path / "memory"
    for leaf, kills in oom_kills.items():
        (mem / leaf).mkdir(parents=True, exist_ok=True)
        (mem / leaf / "memory.oom_control").write_text(
            f"oom_kill_disable 0\nunder_oom 0\noom_kill {kills}\n", encoding="utf-8"
        )
    return tmp_path


def test_a_v1_run_reports_a_breached_init_leaf_without_dmesg():
    """The local cgroup-v1 case: no `memory.events.local`, and a local run has no dmesg either, so
    without a v1 counter this combination has no detector at all.

    `/init` holds its processes directly, so v1's `oom_kill` is charged to the leaf itself.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        root = _v1_leaf_counters(
            __import__("pathlib").Path(d), {"init": 1, "dockerd": 0, "docker": 0}
        )
        results, attach = leaf_oom_report(
            {"CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1"}, b"", cgroup_root=root
        )
    assert [r.name for r in results] == [DIND_LEAF_MEANINGS["init"]]
    assert all(r.status == Result.Status.ERROR for r in results)
    assert attach is True


def test_a_clean_v1_run_stays_green_without_dmesg():
    """The negative arm: a zeroed counter must not manufacture an error."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        root = _v1_leaf_counters(
            __import__("pathlib").Path(d), {"init": 0, "dockerd": 0, "docker": 0}
        )
        results, attach = leaf_oom_report(
            {"CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1"}, b"", cgroup_root=root
        )
    assert results == []
    assert attach is False


def test_the_v2_counter_wins_where_both_files_exist():
    """v2's `oom` is exact for every leaf shape, while v1's `oom_kill` is 0 for `/docker` because
    its containers are children, so a tree carrying both must not be read through the weaker one."""
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        root = __import__("pathlib").Path(d)
        (root / "cgroup.controllers").write_text("memory\n", encoding="utf-8")
        for leaf in DIND_LEAF_MEANINGS:
            (root / leaf).mkdir()
            (root / leaf / "memory.oom_control").write_text(
                "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n", encoding="utf-8"
            )
        (root / "docker" / "memory.events.local").write_text(
            "low 0\nhigh 0\nmax 55\noom 1\noom_kill 0\n", encoding="utf-8"
        )
        for leaf in ("init", "dockerd"):
            (root / leaf / "memory.events.local").write_text(
                "low 0\nhigh 0\nmax 12\noom 0\noom_kill 0\n", encoding="utf-8"
            )
        results = leaf_oom_results(cgroup_root=root)
    assert [r.name for r in results] == [DIND_LEAF_MEANINGS["docker"]]


def test_a_v1_breach_without_dmesg_names_both_undetectable_reports():
    """Neither `/docker` nor the job's cgroup moves its own v1 counter: both are breached by a kill
    charged to a descendant, and by teardown that cgroup is gone.

    Reporting nothing there is indistinguishable from a clean run, so both gaps are named. Naming
    only `/docker` would leave the job-cgroup breach the overcommitted leaf caps allow entirely
    silent.
    """
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        root = _v1_leaf_counters(
            __import__("pathlib").Path(d), {"init": 0, "dockerd": 0, "docker": 0}
        )
        assert dind_unreportable_ooms(_REQUIRED, False, cgroup_root=root) == [
            DIND_LEAF_MEANINGS["docker"],
            DIND_JOB_CGROUP_OOM,
        ]
        # dmesg is the general v1 detector, so with it there is no gap.
        assert dind_unreportable_ooms(_REQUIRED, True, cgroup_root=root) == []


def test_the_named_gaps_are_exactly_what_this_tree_cannot_report(tmp_path):
    """The gap list is only honest if each entry is a report that IS produced with dmesg and is NOT
    produced without it - otherwise it either cries wolf or hides a real blind spot.

    Pins that pairing for the job-cgroup entry on the tree that has it: `_JOB_CGROUP_BREACH_NESTED
    _VICTIM` yields the row, `b""` yields nothing, so `dind_unreportable_ooms` must name it.
    """
    tree = tmp_path
    for leaf in DIND_LEAF_MEANINGS:
        (tree / leaf).mkdir(parents=True)
        (tree / leaf / "memory.oom_control").write_text(
            "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n", encoding="utf-8"
        )
    (tree / "memory.oom_control").write_text(
        "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n", encoding="utf-8"
    )
    with_dmesg, _ = leaf_oom_report(
        _REQUIRED, _JOB_CGROUP_BREACH_NESTED_VICTIM, cgroup_root=tree
    )
    without_dmesg, _ = leaf_oom_report(_REQUIRED, b"", cgroup_root=tree)
    assert [r.name for r in with_dmesg] == [DIND_JOB_CGROUP_OOM]
    assert without_dmesg == []
    assert DIND_JOB_CGROUP_OOM in dind_unreportable_ooms(_REQUIRED, False, cgroup_root=tree)


def test_no_gap_is_claimed_on_v2_or_without_containment(tmp_path):
    """v2's `memory.events.local` is exact for `/docker` too, and a tree with no leaves at all is
    the permissive path rather than a blind spot.

    Containment is asked separately from tree shape: a permissively started v1 daemon can create
    the HOST's `/docker`, which is v1 and has counters, so inferring the permissive case from an
    absent tree names gaps in reports this run never promised."""
    v2 = tmp_path / "v2"
    (v2 / "docker").mkdir(parents=True)
    (v2 / "cgroup.controllers").write_text("memory\n", encoding="utf-8")
    (v2 / "docker" / "memory.events.local").write_text(
        "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n", encoding="utf-8"
    )
    assert dind_unreportable_ooms(_REQUIRED, False, cgroup_root=v2) == []
    assert dind_unreportable_ooms(_REQUIRED, False, cgroup_root=tmp_path / "absent") == []

    # The same v1 tree either way, so only the containment request differs.
    v1 = tmp_path / "v1"
    (v1 / "docker").mkdir(parents=True)
    (v1 / "docker" / "memory.oom_control").write_text(
        "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n", encoding="utf-8"
    )
    assert dind_unreportable_ooms(_REQUIRED, False, cgroup_root=v1) == [
        DIND_LEAF_MEANINGS["docker"],
        DIND_JOB_CGROUP_OOM,
    ], "the required-containment arm must name the gaps, or the next assertion proves nothing"
    assert dind_unreportable_ooms({}, False, cgroup_root=v1) == []


# --- the dmesg backstop is only admissible on a buffer this run owns ----------------------


def test_clear_dmesg_reports_whether_the_buffer_was_cleared():
    """The leaf backstop attributes a kill by leaf name, and leaf names repeat across the jobs a
    runner hosts, so the caller needs to know the clear worked. `Shell.check` already returns a
    bool; discarding it is what left no way to ask."""
    source = inspect.getsource(Utils.clear_dmesg)
    assert "return " in source, "clear_dmesg discards whether the buffer was cleared"
    tree = ast.parse(textwrap.dedent(source))
    returns = [n for n in ast.walk(tree) if isinstance(n, ast.Return) and n.value is not None]
    assert returns, "clear_dmesg has no value-bearing return"
    assert any("Shell.check" in ast.unparse(n.value) for n in returns), [
        ast.unparse(n) for n in returns
    ]


def test_main_keeps_the_clear_result_and_gates_the_leaf_scan_on_it():
    """A failed clear leaves a previous job's records in the buffer; the leaf backstop would then
    name a leaf this run never breached. Asserted on `main` because the bug is the wiring: the
    helper and the matcher are both correct in isolation."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    kept = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Assign) and "clear_dmesg()" in ast.unparse(n.value)
    ]
    assert kept, "main calls clear_dmesg without keeping its result"
    flag = ast.unparse(kept[0].targets[0])

    calls = _main_calls("leaf_oom_report")
    assert len(calls) == 1, f"expected one first-scan call, found {len(calls)}"
    _, args = calls[0]
    buffer_arg = args[1]
    guarded = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Assign)
        and ast.unparse(n.targets[0]) == buffer_arg
        and isinstance(n.value, ast.IfExp)
        and flag in ast.unparse(n.value.test)
    ]
    assert guarded, (
        f"the buffer passed to leaf_oom_report ({buffer_arg!r}) is not gated on {flag!r}, "
        "so an inherited buffer can still name a leaf"
    )
    # Which way round: the buffer is admissible only on the branch where the clear succeeded.
    # Naming the flag is not enough - an inverted conditional reads identically here.
    gate = guarded[0].value
    assert ast.unparse(gate.test) == flag, f"unexpected gate condition: {ast.unparse(gate.test)}"
    assert ast.unparse(gate.body) == "dmesg", (
        f"the cleared branch passes {ast.unparse(gate.body)}, not the buffer this run owns"
    )
    assert not ast.literal_eval(gate.orelse), (
        f"the failed-clear branch passes {ast.unparse(gate.orelse)}, not an empty buffer"
    )


def _requires_true(condition: str, flag: str) -> bool:
    """Whether `condition` can only hold with `flag` true, every other name being true.

    A substring test cannot tell `and dmesg_cleared` from `and not dmesg_cleared`, and those are
    the two readings that matter here, so the expression is evaluated rather than matched.
    """
    expr = ast.parse(condition, mode="eval")
    names = {
        n.id for n in ast.walk(expr) if isinstance(n, ast.Name) and n.id != flag
    }
    if any(
        isinstance(n, (ast.Call, ast.Attribute, ast.Subscript)) for n in ast.walk(expr)
    ):
        return False  # not a pure predicate over names, so it cannot be decided here
    code = compile(expr, "<guard>", "eval")

    def value(flag_value):
        return bool(eval(code, {"__builtins__": {}}, {flag: flag_value, **dict.fromkeys(names, True)}))

    return value(True) and not value(False)


def test_the_host_oom_verdict_is_gated_on_the_clear_but_the_attachment_is_not():
    """The host-wide scan reads the same inherited buffer as the leaf one, so a previous job's
    global kill would otherwise fail this shard. The dump still attaches either way: an
    unattributable buffer is worth reading, it is only worth no verdict."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"
    )

    def guards(node):
        return [
            ast.unparse(n.test)
            for n in ast.walk(main)
            if isinstance(n, ast.If)
            and any(node is d for d in ast.walk(n))
            and n is not node
        ]

    # The appends themselves, not an enclosing `if`: the outermost block that merely contains
    # them is unguarded by design, so keying on the statement is what reads every path to a row.
    rows = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Expr) and "OOM_IN_DMESG_TEST_NAME" in ast.unparse(n)
    ]
    assert rows, "no host-OOM result row in main, so this test proves nothing"
    for row in rows:
        # Which way round, not merely that the flag is named: `and not dmesg_cleared` satisfies a
        # substring check while reporting exactly the inherited kills. Each guard is evaluated as
        # a function of the flag with every other name true, so only a guard the flag must be
        # true for counts.
        assert any(_requires_true(g, "dmesg_cleared") for g in guards(row)), (
            f"no guard a host-OOM verdict needs `dmesg_cleared` true for: {guards(row)}"
        )

    attaches = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Assign)
        and ast.unparse(n.targets[0]) == "attach_dmesg"
        and ast.unparse(n.value) == "True"
    ]
    assert attaches, "nothing attaches the first dump, so the assertion below proves nothing"
    assert any(not any("dmesg_cleared" in g for g in guards(a)) for a in attaches), (
        "every attach is gated on the clear, so a failed clear also loses the dump to read"
    )


def test_a_failed_clear_names_the_host_oom_it_can_no_longer_report():
    """Gating the verdict narrows the signal, so like the leaf case it must not be silent."""
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    clear = source.index("clear_dmesg()")
    scan = source.index("leaf_oom_report(os.environ")
    assert "host OOM is not reportable" in source[clear:scan]


def test_the_late_scan_is_also_gated_on_the_clear():
    """The late scan reads the same host-wide buffer, so it inherits the same defect."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    reads = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.With) and "late_dmesg_file" in ast.unparse(n.items[0])
    ]
    assert len(reads) == 1, f"expected one late-dmesg read, found {len(reads)}"
    gates = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and "dmesg_cleared" in ast.unparse(n.test)
        and any(reads[0] is d for d in ast.walk(n))
    ]
    assert gates, "the late dmesg re-dump is read without knowing the buffer is this run's"


def test_an_inherited_buffer_does_not_manufacture_a_leaf_breach(tmp_path):
    """The behaviour the gate buys, at the boundary the job crosses: identical clean counters,
    identical buffer, and the only difference is whether the buffer is this run's."""
    tree = _leaf_tree(tmp_path, docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    owned, attach = leaf_oom_report(_REQUIRED, _COLLECTIVE_BREACH, cgroup_root=tree)
    assert [r.name for r in owned] == [DIND_LEAF_MEANINGS["docker"]], "the backstop must still fire"
    assert attach is True
    # What `main` passes when the clear failed: the counters remain the sole source.
    assert leaf_oom_report(_REQUIRED, b"", cgroup_root=tree) == ([], False)


def test_a_failed_clear_is_reported_rather_than_silently_narrowing_the_signals():
    """Losing the backstop is a real reduction in coverage, so it must not be silent."""
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    clear = source.index("clear_dmesg()")
    scan = source.index("leaf_oom_report(os.environ")
    assert "WARNING: could not clear dmesg" in source[clear:scan]


def test_the_late_artifact_is_attached_only_when_the_dump_produced_it():
    """`R.files` is uploaded by path, and `upload_result_files_to_s3` records a "File was not
    found" warning on the report for a path that does not exist. The file is written only where
    the redirect ran, so neither an empty buffer nor `is_local_run` answers whether it is there."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    appends = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If) and "R.files.append(LATE_DMESG_LOG)" in ast.unparse(n)
    ]
    assert len(appends) == 1, f"expected one attach site, found {len(appends)}"
    test = ast.unparse(appends[0].test)
    assert "late_dmesg_dumped" in test, f"the attach is not gated on the dump: {test}"

    # And the flag is set only where the redirect succeeded.
    sets = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.Assign)
        and ast.unparse(n.targets[0]) == "late_dmesg_dumped"
        and ast.unparse(n.value) == "True"
    ]
    assert len(sets) == 1, f"expected one place setting the flag true, found {len(sets)}"
    guards = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.If)
        and "Shell.check" in ast.unparse(n.test)
        and "dmesg" in ast.unparse(n.test)
        and any(sets[0] is d for d in ast.walk(n))
    ]
    assert guards, "the flag is set without checking that the re-dump succeeded"


def test_dmesg_availability_is_the_dump_not_an_empty_buffer():
    """A successful dump can be empty - a cleared buffer with nothing since is the normal case -
    so keying availability on the bytes reports a working dmesg as unavailable and emits a blind
    spot warning that does not apply."""
    calls = _main_calls("dind_unreportable_ooms")
    assert len(calls) == 1, f"expected one availability call in main, found {len(calls)}"
    _, args = calls[0]
    assert args == ["os.environ", "late_dmesg_dumped"], (
        f"availability is derived from {args} rather than whether a dump was produced"
    )


# --- The hard-timeout path: the run is being cancelled, so only stdout is kept. ---


def _diag_env(**over):
    env = {
        "CI_DIND_REQUIRE_CGROUP_CONTAINMENT": "1",
        "CI_DIND_INIT_RESERVE": str(16 * GIB),
        "CI_DIND_NESTED_BUDGET": str(40 * GIB),
    }
    env.update(over)
    return env


def test_the_timeout_diagnostics_run_before_the_archive():
    """The defect this exists to remove, and it is an ORDERING one: both the counters and dmesg
    were already collected on the normal path, but downstream of the archive. A cancelled job
    stops mid-archive - measured 13.3 s on job 95139621296 - so anything behind it is never
    emitted, which is why that wedge could not be attributed at all."""
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    diag = source.index("print_timeout_diagnostics(os.environ)")
    archive = source.index("Utils.compress_files_gz(")
    assert diag < archive, "the diagnostics sit behind the archive that the cancellation cuts off"
    # And behind nothing else that can block: the pytest call whose backstop just fired is above.
    assert source.index("run_pytest_and_collect_results(") < diag


def test_the_timeout_diagnostics_are_printed_not_uploaded():
    """An artifact requires the job to live long enough to upload it, which is exactly what a
    cancelled job does not do. The job log is kept regardless, so this must write to stdout and
    must not route through `R.files`/`attached_files`."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    func = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "print_timeout_diagnostics"
    )
    body = ast.unparse(func)
    assert "print(" in body
    for uploader in ("attached_files", "R.files", "compress_files_gz"):
        assert uploader not in body, f"{uploader} cannot survive the cancellation"


def test_the_timeout_diagnostics_only_run_on_a_timed_out_ci_run():
    """A normal run already reports through the artifacts, and dumping the kernel log on every
    green shard is noise. `is_local_run` is excluded because `dmesg -T` is the host's there."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    guards = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.If)
        and any(
            isinstance(c, ast.Call)
            and isinstance(c.func, ast.Name)
            and c.func.id == "print_timeout_diagnostics"
            for c in ast.walk(n)
        )
    ]
    assert len(guards) == 1, f"expected one guarded call, found {len(guards)}"
    test = ast.unparse(guards[0].test)
    assert "hard_killed" in test, f"the diagnostics are not gated on the hard kill: {test}"
    assert "is_local_run" in test, f"the diagnostics are not gated on a CI run: {test}"


def _diagnostics_out(monkeypatch, capsys, tmp_path, dmesg, peaks=None):
    """`print_timeout_diagnostics` stdout with `dmesg -T` faked to return `dmesg`."""
    _peak_tree(tmp_path, peaks if peaks is not None else {"init": 16 * GIB})
    monkeypatch.setattr(
        "ci.jobs.integration_test_job.Shell.get_output",
        lambda command, **kw: dmesg if command == "dmesg -T" else "",
    )
    capsys.readouterr()
    print_timeout_diagnostics(_diag_env(), cgroup_root=tmp_path)
    return capsys.readouterr().out


def test_the_timeout_diagnostics_report_the_peaks_and_the_kernel_kills(
    monkeypatch, capsys, tmp_path
):
    """Both halves are load-bearing and answer different questions: the counters say which leaf was
    pinned at its cap, dmesg says whether the kernel killed anything at all - which the counters
    cannot answer on v1, where the kill is charged to a victim in a descendant cgroup."""
    out = _diagnostics_out(
        monkeypatch,
        capsys,
        tmp_path,
        "kernel: [1] some unrelated line\n"
        "kernel: oom-kill:constraint=CONSTRAINT_MEMCG,oom_memcg=/init,task_memcg=/init/x\n",
    )
    init_line = next(l for l in out.splitlines() if "/init" in l and "peak" in l)
    assert "AT CAP" in init_line, init_line
    assert "oom_memcg=/init" in out, out
    assert "some unrelated line" not in out, "the whole buffer is not the diagnostics"


def test_the_timeout_diagnostics_say_so_when_the_kernel_killed_nothing(
    monkeypatch, capsys, tmp_path
):
    """Silence is the ambiguity this path exists to remove: with no line printed, a reader cannot
    tell a run with no kernel kill from one whose diagnostics never got emitted."""
    out = _diagnostics_out(monkeypatch, capsys, tmp_path, "kernel: nothing of interest\n")
    assert "No kernel memory kill in dmesg" in out, out


def test_the_timeout_diagnostics_distinguish_unreadable_dmesg_from_no_kill(
    monkeypatch, capsys, tmp_path
):
    """`Shell.get_output` returns an empty string both when dmesg is empty and when the command
    failed, so reporting "no kill" on it would claim a negative the run cannot support."""
    out = _diagnostics_out(monkeypatch, capsys, tmp_path, "")
    assert "no dmesg available" in out, out
    assert "No kernel memory kill in dmesg" not in out, out


def test_the_timeout_diagnostics_report_a_cgroup_kill_not_only_a_host_one(
    monkeypatch, capsys, tmp_path
):
    """A capped leaf's kill is `CONSTRAINT_MEMCG`, which `HOST_OOM_DMESG_PATTERNS` deliberately
    excludes - it selects global kills only. Reusing that tuple here would print nothing for
    precisely the kills this PR's caps create."""
    memcg = "kernel: oom-kill:constraint=CONSTRAINT_MEMCG,oom_memcg=/docker,task_memcg=/docker/c\n"
    assert not any(p in memcg.encode() for p in HOST_OOM_DMESG_PATTERNS), (
        "this line must NOT match the host-only patterns, or the test proves nothing"
    )
    out = _diagnostics_out(monkeypatch, capsys, tmp_path, memcg)
    assert "oom_memcg=/docker" in out, out


def test_the_timeout_diagnostics_derive_no_result_row(monkeypatch, capsys, tmp_path):
    """This dmesg is neither cleared nor scoped, so leaf names in it may belong to another job on
    the same runner. Printing an unattributable buffer is useful; failing a shard on it is not -
    `leaf_oom_report` owns the rows, on a buffer whose ownership it can establish."""
    assert (
        _diagnostics_out(
            monkeypatch,
            capsys,
            tmp_path,
            "kernel: oom-kill:constraint=CONSTRAINT_MEMCG,oom_memcg=/init,task_memcg=/init\n",
        )
        is not None
    )
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    func = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "print_timeout_diagnostics"
    )
    # The docstring only, dropped: it names these very helpers to say it does NOT call them, so
    # scanning the whole node matches its own prose. Dropping every `Expr` instead would also drop
    # the `print` calls, leaving nothing to scan and an assertion that passes on any body.
    statements = func.body[1:] if ast.get_docstring(func) else func.body
    body = "\n".join(ast.unparse(n) for n in statements)
    assert "print(" in body, "nothing left to scan, so the assertions below prove nothing"
    for reporter in ("Result(", "leaf_oom_report", "set_error", "has_error"):
        assert reporter not in body, f"{reporter} would fail a shard on an unscoped buffer"


def test_the_oom_markers_cover_the_kills_the_leaf_caps_produce():
    """The markers are matched against raw dmesg text, so a kernel spelling that is not in the
    tuple is a silent blind spot on exactly the timed-out path that has no other evidence."""
    for line in (
        "oom-kill:constraint=CONSTRAINT_MEMCG,oom_memcg=/init,task_memcg=/init",
        "oom-kill:constraint=CONSTRAINT_NONE,oom_memcg=(null)",
        "Out of memory: Killed process 1234 (clickhouse)",
        "oom_reaper: reaped process 1234 (clickhouse)",
    ):
        assert any(m in line for m in OOM_DMESG_MARKERS), line
    assert not any(m in "kernel: usb 1-1: new high-speed device" for m in OOM_DMESG_MARKERS)


def test_the_archive_is_bounded_only_on_the_timed_out_path():
    """An archive that outlives the cancellation yields no artifact while consuming the window,
    so on that path it must be bounded and non-fatal. On every other path it has the time it
    needs and a failure to produce it is a real error, which `strict` must keep reporting."""
    calls = _main_calls("compress_files_gz")
    assert not calls, "expected the archive to be built through a bound-carrying loop in main"
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    archive = source.index("Utils.compress_files_gz(")
    block = source[archive : archive + 400]
    assert "timeout=remaining" in block, block
    assert "strict=not hard_killed" in block, block
    bound = source.index("archive_deadline = ")
    decl = source[bound : source.index("\n            for files", bound)]
    assert "if hard_killed" in decl and "else None" in decl, decl
    assert "TIMED_OUT_ARCHIVE_TIMEOUT" in decl, decl


def test_all_the_archives_share_one_deadline():
    """Two archives are written, so a per-archive bound of N gives the pair 2N and can outlive the
    window it was sized against - the pair, not one archive, is what has to fit. A deadline taken
    once before the loop is what makes the second archive inherit what the first left."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    loops = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.For) and "compress_files_gz" in ast.unparse(n)
    ]
    assert len(loops) == 1, f"expected one archiving loop, found {len(loops)}"
    assert "archive_deadline" not in ast.unparse(loops[0].iter)
    body = ast.unparse(loops[0].body)
    # Recomputed per archive FROM the shared deadline, not re-taken from the constant.
    assert "archive_deadline - time.monotonic()" in body, body
    assert "TIMED_OUT_ARCHIVE_TIMEOUT" not in body, body


def test_a_bounded_command_that_finished_in_time_reports_no_timeout(tmp_path):
    """Nothing cancels the timeout thread, so a bounded command that succeeded still woke up and
    signalled its old process group id. Harmless while that id is unreused, but it prints
    `Timeout exceeded` into the job log this change exists to make readable, which is worse than
    no diagnostics: it names a timeout that did not happen."""
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys, time\n"
        f"sys.path.insert(0, {os.path.join(os.path.dirname(__file__), '..', '..')!r})\n"
        "from ci.praktika.utils import Utils\n"
        f"print('RESULT=', Utils.compress_files_gz(['/etc/hostname'],"
        f" {str(tmp_path / 'ok.tar.gz')!r}, timeout=3))\n"
        "time.sleep(6)\n"
        "print('DONE')\n",
        encoding="utf-8",
    )
    p = subprocess.run(
        [sys.executable, str(probe)], capture_output=True, text=True, timeout=90
    )
    out = p.stdout + p.stderr
    assert "DONE" in out, out
    assert "Timeout exceeded" not in out, out


def test_a_bounded_command_whose_descendant_outlives_it_is_still_timed_out(tmp_path):
    """The other half of the same wake-up. `Shell.run` returns only once the output readers do,
    and they return when the last writer closes the pipe, so a backgrounded descendant keeps the
    call blocked long past the leader's exit. A watchdog retired on the leader alone leaves that
    group unsignalled and the bound unenforced - which is the archive bound above."""
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys, time\n"
        f"sys.path.insert(0, {os.path.join(os.path.dirname(__file__), '..', '..')!r})\n"
        "from ci.praktika.utils import Shell\n"
        "t0 = time.monotonic()\n"
        "Shell.run('sleep 60 &', timeout=2, verbose=False)\n"
        "print('ELAPSED=%.1f' % (time.monotonic() - t0))\n",
        encoding="utf-8",
    )
    p = subprocess.run(
        [sys.executable, str(probe)], capture_output=True, text=True, timeout=120
    )
    out = p.stdout + p.stderr
    elapsed = float(re.search(r"ELAPSED=([\d.]+)", out).group(1))
    # Generous: the point is 2 s versus the descendant's own 60 s, not the exact wake-up latency.
    assert elapsed < 30, f"the bound was not enforced, took {elapsed}s\n{out}"
    assert "Timeout exceeded" in out, out


def test_the_shared_archive_deadline_completes_an_ordinary_archive():
    """Sizing this to the 13.3 s the motivating run had before cancellation is the wrong target,
    and measurably so: 413 MiB of logs takes ~15 s to archive, so no bound both fits that window
    and produces an artifact. The bound exists to stop a STUCK archive from running to the outer
    job deadline; the diagnostics beat the cancellation by running before it, not by racing it.

    So the floor matters as much as the ceiling: too small and every hard-killed run loses logs
    it had time to write, on a healthy host where the backstop fired but nothing is cancelling."""
    assert 60 <= TIMED_OUT_ARCHIVE_TIMEOUT <= 600, (
        f"{TIMED_OUT_ARCHIVE_TIMEOUT}s does not cover an ordinary archive while still bounding it"
    )


def test_a_timeout_in_any_pytest_run_that_precedes_the_archive_sets_the_flag():
    """`hard_killed` gates both the diagnostics and the bounded archive, so a run whose backstop
    fired must set it whichever call produced it. The bugfix-validation calls bound their result
    to `_`, which left the flag false on exactly the timeout the diagnostics exist for.

    The retries call is excluded deliberately: it runs after both, so its result cannot reach
    them - asserted here rather than assumed, since moving it earlier would reopen the gap.
    """
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    # Line numbers, not source offsets: `ast.unparse` reformats, so its output cannot be found
    # back in the file and an `index` on it raises rather than ordering anything.
    diagnostics = min(
        n.lineno
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "print_timeout_diagnostics"
    )
    runs = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Assign)
        and "run_pytest_and_collect_results" in ast.unparse(n.value)
    ]
    assert len(runs) == 5, f"expected five pytest runs in main, found {len(runs)}"
    folded = {
        flag: {
            ast.unparse(n.value).removeprefix(f"{flag} or ")
            for n in ast.walk(main)
            if isinstance(n, ast.Assign)
            and ast.unparse(n.targets[0]) == flag
            and ast.unparse(n.value).startswith(f"{flag} or ")
        }
        for flag in ("timed_out", "hard_killed")
    }
    assert folded["timed_out"] and folded["hard_killed"], "a flag is folded nowhere"
    checked = 0
    for run in runs:
        # Only the runs the diagnostics and the archive are still downstream of must feed them.
        if run.lineno > diagnostics:
            continue
        # Tuple elements read from the AST: unparsing the whole target and splitting its text
        # leaves the tuple's punctuation attached to the names.
        elts = [ast.unparse(e) for e in run.targets[0].elts]
        assert len(elts) == 3, f"a pytest run does not bind both flags: {elts}"
        for flag, bound in (("timed_out", elts[1]), ("hard_killed", elts[2])):
            assert bound != "_", f"a pytest run before the diagnostics discards {flag}"
            assert bound in folded[flag], f"{bound} is bound but never folded into {flag}"
        checked += 1
    assert checked == 4, f"expected four runs ahead of the diagnostics, found {checked}"


def test_only_the_hard_backstop_counts_as_an_imminent_cancellation():
    """The graceful xdist session-timeout is an ordinary budgeted stop with hours of the job's
    5 h deadline left, so shortening the archive there discards logs for no reason. Only the
    subprocess backstop - which fires after the session-timeout was already missed, i.e. a hung
    run - means the runner is about to cancel. `timed_out` covers both, so it is the wrong gate."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    func = next(
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "run_pytest_and_collect_results"
    )
    sets_hard = [
        n
        for n in ast.walk(func)
        if isinstance(n, ast.Assign)
        and ast.unparse(n.targets[0]) == "hard_killed"
        and ast.unparse(n.value) == "True"
    ]
    assert len(sets_hard) == 1, f"expected one hard-kill assignment, found {len(sets_hard)}"
    # It must live on the backstop branch, whose test is the elapsed-vs-timeout comparison, and
    # NOT on the branch the graceful `session-timeout` marker selects.
    branches = [
        n
        for n in ast.walk(func)
        if isinstance(n, ast.If) and any(sets_hard[0] is d for d in ast.walk(n))
    ]
    assert branches, "the hard-kill flag is set unconditionally"
    innermost = min(branches, key=lambda n: len(ast.unparse(n)))
    test = ast.unparse(innermost.test)
    assert "run_sw.duration" in test and "timeout" in test, test
    assert "session-timeout" not in test, test
    # And the graceful branch must still set `timed_out`, so the SKIPPED downgrade is unchanged.
    graceful = next(
        n
        for n in ast.walk(func)
        if isinstance(n, ast.If) and "dsession.Interrupted" in ast.unparse(n.test)
    )
    body = ast.unparse(graceful.body)
    assert "timed_out = True" in body, body
    assert "hard_killed" not in body, body


def test_the_archive_bound_is_actually_enforced(tmp_path):
    """The call site passing `timeout=` proves nothing by itself: a version of
    `compress_files_gz` that accepts the argument and drops it satisfies every source-level
    assertion above. Measure the kill instead. An archive writing into a FIFO nobody reads blocks
    indefinitely, which is the shape a cancelled runner leaves behind.

    Run out of process with its own deadline, because an unenforced bound does not return: in
    process it would hang the suite, and a hang is not a failure - a green-or-hang oracle reports
    nothing about the run that hangs.
    """
    blocked = tmp_path / "reader-less.fifo"
    os.mkfifo(blocked)
    probe = tmp_path / "probe.py"
    probe.write_text(
        "import sys, time\n"
        f"sys.path.insert(0, {os.path.join(os.path.dirname(__file__), '..', '..')!r})\n"
        "from ci.praktika.utils import Utils\n"
        "started = time.monotonic()\n"
        f"result = Utils.compress_files_gz([{_JOB_SCRIPT!r}], {str(blocked)!r},"
        " timeout=5, strict=False)\n"
        'print(f"RESULT={result} ELAPSED={time.monotonic() - started:.1f}")\n',
        encoding="utf-8",
    )
    try:
        p = subprocess.run(
            [sys.executable, str(probe)], capture_output=True, text=True, timeout=90
        )
    except subprocess.TimeoutExpired:
        pytest.fail("the bound was not enforced: the archive never returned")
    assert "RESULT=None" in p.stdout, p.stdout + p.stderr
    elapsed = float(re.search(r"ELAPSED=([\d.]+)", p.stdout).group(1))
    assert elapsed >= 5, f"returned in {elapsed:.1f}s, before the bound it was given"


def test_a_bounded_archive_that_fails_does_not_lose_the_rest_of_the_run():
    """`compress_files_gz` returned the path unconditionally, so a caller that stops raising gets
    `None` appended and the report then warns "File was not found" on it. It also must not abort
    the job: on the timed-out path everything after this still has to run."""
    dest = "/dev/full/unwritable.tar.gz"
    src = _JOB_SCRIPT
    assert Utils.compress_files_gz([src], dest, timeout=30, strict=False) is None
    with pytest.raises(RuntimeError):
        Utils.compress_files_gz([src], dest, timeout=30)


def test_a_bounded_archive_returns_the_path_when_it_succeeds(tmp_path):
    """The bound must not change the normal outcome: a caller appends this to `attached_files`."""
    dest = str(tmp_path / "ok.tar.gz")
    assert Utils.compress_files_gz([_JOB_SCRIPT], dest, timeout=120) == dest
    assert os.path.getsize(dest) > 0


def test_the_archive_bound_leaves_room_for_the_diagnostics():
    """The whole point is that the diagnostics are emitted first and the archive is best effort,
    so the bound has to be well inside the window a cancelled job gets, not a second budget that
    can itself outlive it."""
    assert 0 < TIMED_OUT_ARCHIVE_TIMEOUT <= 600


def test_the_dmesg_dump_degrades_rather_than_aborting_the_reports_it_feeds():
    """It was `strict=True`, which raises out of `main` and takes the counter-based reports with
    it - and those need no dmesg at all. A failed dump must narrow the signals, not remove them."""
    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    dump = source.index('Shell.check("dmesg -T > ./ci/tmp/dmesg.log"')
    call = source[dump : source.index("\n", dump)]
    assert "strict=True" not in call, call
    report = source.index("leaf_results, attach_dmesg = leaf_oom_report(os.environ, ")
    assert "WARNING: could not dump dmesg" in source[dump:report]


def test_a_hard_killed_bugfix_run_stops_scheduling_the_remaining_build_types():
    """Bugfix validation runs up to four build types in sequence, and its guard is
    `any(not r.is_ok() ...)`. A hard-killed run usually produces NO results, and `any` over an
    empty list is False, so the loop kept going: the job could burn a second and third backstop
    before reaching the diagnostics, which is the opposite of emitting them promptly."""
    tree = ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read())
    main = next(
        n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    loops = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.For) and "bugfix_bt" == ast.unparse(n.target)
    ]
    assert len(loops) == 1, f"expected one build-type loop, found {len(loops)}"
    guards = [
        n
        for n in ast.walk(loops[0])
        if isinstance(n, ast.If)
        and any(isinstance(b, ast.Break) for b in ast.walk(n))
        and ("hard_killed" in ast.unparse(n.test) or "bt_test_results" == ast.unparse(n.test))
    ]
    # Entry, not only continuation: `all` over an empty list is True, so a hard-killed primary
    # run passes the entry test and the first extra build type runs before any break is reached.
    entry = next(
        n
        for n in ast.walk(main)
        if isinstance(n, ast.If) and any(loops[0] is d for d in ast.walk(n))
        and "is_ok()" in ast.unparse(n.test)
    )
    entry_test = ast.unparse(entry.test)
    assert "hard_killed" in entry_test, f"the loop is entered after a hard kill: {entry_test}"
    assert "test_results and" in entry_test or "not test_results" in entry_test, (
        f"an empty primary result still enters the loop: {entry_test}"
    )
    tests = [ast.unparse(n.test) for n in guards]
    assert any("hard_killed" in t for t in tests), (
        f"no break on a hard kill, so the loop schedules more runs after one: {tests}"
    )
    assert any("not bt_test_results" in t for t in tests), (
        f"no break on an empty result, which is what a hard-killed run leaves: {tests}"
    )


# --- bounding the pulls the daemon must hold at once ---------------------------------------
# The prefetch launched one `docker pull` per image, all at once, and each pull costs the daemon
# up to `--max-concurrent-downloads` concurrent layer decompressions in its own address space.
# The widest batch therefore asked a capped leaf for far more than the daemon's resting
# footprint, and only the widest batch's shard died. These pin the bound and the contract it
# must not change: a failed pull still fails the job, an arch mismatch still does not.

_PREFETCH_SCRIPT = os.path.join(
    os.path.dirname(__file__), "..", "jobs", "scripts", "prefetch-integration-test-images"
)

# The widest batch observed in CI, and the only shard width whose daemon died. The bound has to
# be below it, or the batch that failed is still pulled all at once.
WIDEST_OBSERVED_BATCH = 25

# Records an interval per pull so peak concurrency is computed from the intervals afterwards.
# A live counter would be read and written by every child at once and undercount.
_FAKE_DOCKER = """#!/bin/bash
S=$(date +%s.%N)
sleep "${FAKE_PULL_SECS:-0.2}"
printf '%s %s %s\\n' "$S" "$(date +%s.%N)" "$2" >> "$INTERVALS"
if [ -n "${SKIP_IMAGE:-}" ] && [ "$2" = "$SKIP_IMAGE" ]; then
    echo "no matching manifest for linux/arm64" >&2; exit 1
fi
if [ -n "${FAIL_IMAGE:-}" ] && [ "$2" = "$FAIL_IMAGE" ]; then
    echo "unexpected EOF" >&2; exit 1
fi
# What a leaf OOM does to a pull: the wrapper subshell dies before either outcome file is
# written, which no exit status can report because freeing a slot already reaped the child.
# Walk up to it rather than killing $PPID, which is only the `timeout` around this process:
# docker <- timeout <- the $(...) capturing it <- pull_one's caller <- the wrapper subshell.
if [ -n "${KILL_IMAGE:-}" ] && [ "$2" = "$KILL_IMAGE" ]; then
    victim=$PPID
    for _ in 1 2; do victim=$(awk '{print $4}' "/proc/$victim/stat"); done
    kill -9 "$victim"; sleep 5
fi
echo "Status: Downloaded newer image for $2"
"""


def _run_prefetch(
    tmp_path, images, parallel=None, skip="", fail="", kill="", script=None
):
    """`(returncode, stdout, peak_concurrency)` from running the prefetch against a fake docker."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    fake = bin_dir / "docker"
    fake.write_text(_FAKE_DOCKER)
    fake.chmod(0o755)
    intervals = tmp_path / "intervals"
    intervals.write_text("")

    env = {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "INTERVALS": str(intervals),
        "FAKE_PULL_SECS": "0.2",
        "PULL_RETRIES": "2",
        "SKIP_IMAGE": skip,
        "FAIL_IMAGE": fail,
        "KILL_IMAGE": kill,
    }
    if parallel is not None:
        env["PULL_PARALLEL"] = str(parallel)

    p = subprocess.run(
        ["bash", script or _PREFETCH_SCRIPT, *images],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )

    events = []
    for line in intervals.read_text().splitlines():
        parts = line.split()
        if len(parts) >= 3:
            events.append((float(parts[0]), 1))
            events.append((float(parts[1]), -1))
    events.sort()
    current = peak = 0
    for _, delta in events:
        current += delta
        peak = max(peak, current)
    return p.returncode, p.stdout, peak


def test_the_prefetch_holds_no_more_pulls_open_than_its_bound(tmp_path):
    """The bound is on pulls IN FLIGHT, so it must hold however wide the batch is.

    Driven from the production constant, not a literal: pinning the literal here would stay green
    if that constant were raised back to the width of the batch.
    """
    images = [f"img{i}" for i in range(1, WIDEST_OBSERVED_BATCH + 1)]
    rc, out, peak = _run_prefetch(tmp_path, images, parallel=PREFETCH_PARALLEL_PULLS)
    assert rc == 0, out
    assert (
        peak == PREFETCH_PARALLEL_PULLS
    ), f"expected at most {PREFETCH_PARALLEL_PULLS} pulls in flight, saw {peak}"
    assert out.count("Status: Downloaded") == 0, "the fake docker's output is captured, not echoed"


def test_every_image_is_still_pulled_under_the_bound(tmp_path):
    """Throttling must not drop an image: a missing pull is a test that fails later, mid-run."""
    images = [f"img{i}" for i in range(1, 26)]
    rc, out, _ = _run_prefetch(tmp_path, images, parallel=8)
    assert rc == 0, out
    for image in images:
        assert f"Pulled {image} in" in out, f"{image} was never pulled"


def test_the_bound_is_a_maximum_not_a_batch_size(tmp_path):
    """Fewer images than slots must not wait for a full batch, and 0 must mean unbounded."""
    assert _run_prefetch(tmp_path, ["a", "b", "c"], parallel=8)[2] == 3
    images = [f"img{i}" for i in range(1, 26)]
    assert _run_prefetch(tmp_path, images, parallel=0)[2] == len(images)
    assert _run_prefetch(tmp_path, images, parallel=1)[2] == 1


def test_a_failed_pull_still_fails_the_job_under_the_bound(tmp_path):
    """The reap that frees a slot consumes a child's exit status, so the outcome cannot come from
    a later `wait`. Whichever position the failure takes, the script must still fail."""
    images = [f"img{i}" for i in range(1, 26)]
    for position in ("img1", "img13", "img25"):
        rc, out, _ = _run_prefetch(tmp_path, images, parallel=8, fail=position)
        assert rc == 1, f"a failed pull of {position} finished the prefetch green: {out}"
        assert "ERROR: Failed to pull" in out
        assert position in out.split("ERROR: Failed to pull")[1]


def test_a_pull_killed_before_it_records_an_outcome_fails_the_job(tmp_path):
    """The condition an exit status can no longer report, and the reason the outcome files exist.

    Freeing a slot reaps a child, so a pull whose shell is killed before writing either file, the
    shape a leaf OOM takes, leaves no status for any later `wait` to find. Silence there would
    finish the prefetch green having never pulled the image.
    """
    images = [f"img{i}" for i in range(1, WIDEST_OBSERVED_BATCH + 1)]
    rc, out, _ = _run_prefetch(
        tmp_path, images, parallel=PREFETCH_PARALLEL_PULLS, kill="img13"
    )
    assert rc == 1, f"a pull killed mid-flight finished the prefetch green: {out}"
    assert "No pull outcome recorded for" in out
    assert "img13" in out.split("No pull outcome recorded for")[1]
    # The other 24 must still be pulled: this path reports, it does not abort the batch.
    assert out.count("Pulled ") == len(images) - 1


def test_a_killed_pull_is_not_hidden_by_a_namesake(tmp_path):
    """Outcomes must be keyed injectively. `foo:bar` and `foo/bar` are distinct references that
    sanitize to one filename, so keying by name lets a killed pull read as its namesake's
    success: the batch finishes green having never pulled it."""
    images = ["reg.example/foo:bar", "reg.example/foo/bar"]
    assert len({re.sub(r"[^a-zA-Z0-9._-]", "_", i) for i in images}) == 1, (
        "these names no longer collide once sanitized, so this test no longer covers the case"
    )
    rc, out, _ = _run_prefetch(tmp_path, images, parallel=2, kill=images[0])
    assert rc == 1, f"a killed pull was hidden by its namesake: {out}"
    assert "No pull outcome recorded for" in out
    assert images[0] in out.split("No pull outcome recorded for")[1]


def test_the_integration_job_cache_key_covers_the_prefetch_script(tmp_path):
    """The job runs this script, so a change to it must invalidate the cached result: otherwise a
    prefetch-only change reuses a green record produced by the previous version."""
    assert (
        "./ci/jobs/scripts/prefetch-integration-test-images"
        in common_integration_test_job_config.digest_config.include_paths
    ), "the prefetch script is not part of the integration job's cache key"


def test_an_arch_mismatch_still_does_not_fail_the_job_under_the_bound(tmp_path):
    """`no matching manifest` is an expected outcome on a runner of the other architecture."""
    images = [f"img{i}" for i in range(1, 26)]
    rc, out, _ = _run_prefetch(tmp_path, images, parallel=8, skip="img7")
    assert rc == 0, out
    assert "SKIP img7" in out

    # And it must not mask a real failure alongside it.
    rc, out, _ = _run_prefetch(tmp_path, images, parallel=8, skip="img7", fail="img13")
    assert rc == 1, out
    assert "SKIP img7" in out and "img13" in out


def test_the_bound_the_job_uses_actually_limits_the_widest_batch(tmp_path):
    """A bound at or above the widest batch is not a bound: that batch is still pulled at once.

    `> 0` is not enough to assert, since 0 and 25 are both positive and both leave the observed
    failing batch unthrottled.
    """
    assert PREFETCH_PARALLEL_PULLS > 0, "a bound of 0 restores the unbounded fan-out"
    assert PREFETCH_PARALLEL_PULLS < WIDEST_OBSERVED_BATCH, (
        f"a bound of {PREFETCH_PARALLEL_PULLS} does not throttle the widest observed batch of "
        f"{WIDEST_OBSERVED_BATCH}"
    )

    # The effective argument, not just the presence of a parameter: a caller passing its own
    # bound would pass every assertion above while pulling the whole batch at once. Read from
    # the AST rather than `_main_calls`, which unparses keyword VALUES and drops their names.
    main = next(
        n
        for n in ast.walk(ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read()))
        if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    prefetch_calls = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "prefetch_images"
    ]
    assert (
        len(prefetch_calls) == 1
    ), f"expected one prefetch call site, found {len(prefetch_calls)}"
    call = prefetch_calls[0]
    overrides = [k for k in call.keywords if k.arg == "parallel"] + call.args[3:]
    assert not overrides, (
        "main passes its own bound instead of the reviewed default: "
        f"{ast.unparse(call)}"
    )

    source = open(_JOB_SCRIPT, encoding="utf-8").read()
    assert '"PULL_PARALLEL": str(parallel)' in source, "the job never passes a bound"

    # The script's own default must be the same bound, so running it by hand is throttled too.
    images = [f"img{i}" for i in range(1, WIDEST_OBSERVED_BATCH + 1)]
    assert _run_prefetch(tmp_path, images)[2] == PREFETCH_PARALLEL_PULLS


def test_a_prefetch_failure_reports_the_leaf_that_died(tmp_path, monkeypatch):
    """The pre-pull failure exits through `complete_job`, so the end-of-run leaf scan never runs
    on that path: without these rows a daemon killed by its own cap reports only an unexplained
    infrastructure error. Reverting the early exit to a bare `ERROR` reddens this."""
    breached = _leaf_tree(
        tmp_path / "breached", docker=_NO_OOM, init=_NO_OOM, dockerd=_OWN_LIMIT_OOM
    )
    result = prefetch_failure_result(env=_REQUIRED, cgroup_root=breached)
    assert result.status == Result.Status.ERROR
    names = [r.name for r in result.results or []]
    assert names == [
        DIND_LEAF_MEANINGS["dockerd"]
    ], f"the breached daemon leaf is not named in the report: {names}"

    # A clean run must not invent a row, or every pull failure would read as a resource kill.
    clean = _leaf_tree(tmp_path / "clean", docker=_NO_OOM, init=_NO_OOM, dockerd=_NO_OOM)
    assert not prefetch_failure_result(env=_REQUIRED, cgroup_root=clean).results

    # Without containment these are the HOST's cgroups, so they must not be read at all.
    assert not prefetch_failure_result(env={}, cgroup_root=breached).results

    # The daemon's own log carries the containment decision and any refusal, and is the one
    # artifact this path used to lose: it is attached whenever it exists.
    import ci.jobs.integration_test_job as job

    log = tmp_path / "docker-in-docker.log"
    log.write_text("containment active\n", encoding="utf-8")
    monkeypatch.setattr(job, "DOCKER_IN_DOCKER_LOG", str(log))
    attached = prefetch_failure_result(env=_REQUIRED, cgroup_root=breached).files
    assert attached == [str(log)], f"the daemon log is not attached: {attached}"

    # A path that does not exist must not be attached: the upload would fail on it, and a daemon
    # that refused to start before opening its log is exactly when this path runs.
    monkeypatch.setattr(job, "DOCKER_IN_DOCKER_LOG", str(tmp_path / "absent.log"))
    absent = prefetch_failure_result(env=_REQUIRED, cgroup_root=breached).files
    assert not absent, f"a nonexistent daemon log was attached anyway: {absent}"

    # And the early exit must COMPLETE the diagnostic result: computing it and dropping it on the
    # floor leaves the job reporting nothing, so the call has to be `complete_job`'s receiver.
    main = next(
        n
        for n in ast.walk(ast.parse(open(_JOB_SCRIPT, encoding="utf-8").read()))
        if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    completed = [
        n
        for n in ast.walk(main)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "complete_job"
        and isinstance(n.func.value, ast.Call)
        and isinstance(n.func.value.func, ast.Name)
        and n.func.value.func.id == "prefetch_failure_result"
    ]
    assert len(completed) == 1, (
        "the prefetch bail does not complete the diagnostic result, so the rows it computes "
        "never reach the job report"
    )
