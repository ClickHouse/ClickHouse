import argparse
import os
import re
import shlex
import subprocess
import time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ci.jobs.scripts.bugfix_validation import bugfix_build_types, find_master_builds
from ci.jobs.scripts.find_tests import Targeting
from ci.jobs.scripts.integration_tests_configs import (
    IMAGES_ENV,
    LLVM_COVERAGE_SKIP_PREFIXES,
    force_heavy_modules_sequential,
    get_optimal_test_batch,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

repo_dir = Utils.cwd()
temp_path = f"{repo_dir}/ci/tmp"

# Must equal helpers/cluster.py's RABBITMQ_RECREATE_TOKEN, which emits it. Copied
# rather than imported so this script does not depend on the test helpers' imports;
# test_cluster_waiters/test_rabbitmq_start_retry.py asserts the two stay equal.
RABBITMQ_RECREATE_TOKEN = "RABBITMQ_RECREATE"


MAX_FAILS_BEFORE_DROP = 5
# Flaky-check best-effort scope cap: the maximum number of changed test modules a single
# flaky-check run will execute. A PR can mechanically touch a large number of integration
# test modules (e.g. a repo-wide lint/format change), and running every one of them
# repeatedly under `--dist=each` cannot fit the flaky-check time budget - the job would be
# hard-killed by the external CI timeout before producing any report. When the cap is
# exceeded the extra modules are skipped (best effort) and the selected ones get full
# flakiness coverage instead of a truncated run. See FLAKY_CHECK_TIME_LIMIT for the hard
# time guarantee that backstops this.
MAX_FLAKY_CHECK_MODULES = 10
OOM_IN_DMESG_TEST_NAME = "OOM in dmesg"

# The post-coverage-merge dmesg scan's own file. Distinct from `./ci/tmp/dmesg.log`, which the
# first scan may already have queued for upload: both dumps redirect, and `>` truncates on open.
LATE_DMESG_LOG = "./ci/tmp/dmesg-after-merge.log"

# The kernel record for the whole run, captured as it is emitted. A snapshot cannot stand in for
# it: the container churn below logs several kernel lines per veth pair, which wraps the ring
# buffer many times over in a run. Its own path, because `on_error_hook` truncates `dmesg.log`.
DMESG_FOLLOW_LOG = "./ci/tmp/dmesg-follow.log"

# Rides a dmesg-derived NEGATIVE that is not a proven one, where `oom_memcg` scoping cannot help:
# a record that starts after the kill simply does not hold it.
PARTIAL_DMESG_CAVEAT = (
    " (the record does not cover the whole run, so an earlier kill would not be in it)"
)

# Rides the POSITIVE, which is unsound in the opposite direction: a buffer still holding a
# previous job's records can show a kill that is not this run's, while its silence still covers it.
UNCLEARED_DMESG_CAVEAT = (
    " (buffer not cleared for this run, so a kill may be a previous job's)"
)

# `docker_in_docker.sh`'s own output, which holds the containment decision and any refusal.
DOCKER_IN_DOCKER_LOG = "./ci/tmp/docker-in-docker.log"

# Images pulled at once. This bounds the daemon's anon working set, which does follow the pulls in
# flight: measured against the widest batch, 161 MiB at one concurrent pull and 697 MiB at 25. It
# does not bound the leaf's total charge, because each layer is also written through the leaf's page
# cache and stays charged there until writeback lands, whatever the concurrency. Sizing the leaf for
# that is `INTEGRATION_DIND_DAEMON_LIMIT`'s job, not this knob's.
PREFETCH_PARALLEL_PULLS = 8

# Seconds ALL archiving gets, together, after the hard backstop fired. Sized to complete an
# ordinary archive (measured: 15 s for 413 MiB of logs) and to bound a stuck one, not to fit
# inside a cancellation window - no useful bound does, so the diagnostics run first instead.
TIMED_OUT_ARCHIVE_TIMEOUT = 120

# A host OOM exhausts the whole machine, so any of these is a job-level failure. Both are
# global-only; `oom_reaper: reaped process` is excluded because the kernel prints it for a
# cgroup kill too, which would report every capped leaf's kill as a host OOM as well.
HOST_OOM_DMESG_PATTERNS = (
    b"Out of memory: Killed process",
    b"oom-kill:constraint=CONSTRAINT_NONE",
)

# Kernel records a memory kill is diagnosed from, whichever scope it happened in. Broader than
# `HOST_OOM_DMESG_PATTERNS`, which selects only the global ones: here a cgroup kill is wanted
# too, and `oom_reaper` is kept because the surviving lines are read rather than classified.
OOM_DMESG_MARKERS = ("oom-kill:", "Out of memory:", "oom_reaper:")

# The cgroup leaves `docker_in_docker.sh` creates, and what a kill in each one means. The paths
# are unqualified because the script only runs under `--cgroupns=private`.
DIND_CGROUP_ROOT = "/sys/fs/cgroup"
DIND_LEAF_MEANINGS = {
    "docker": "Container memory budget exceeded (/docker)",
    "init": "Harness memory limit exceeded (/init)",
    "dockerd": "Docker daemon memory limit exceeded (/dockerd)",
}

# The leaf caps may sum above the job limit, so the job's own cgroup can be what breaches. That is
# neither a leaf nor a host OOM, so both other detectors ignore it by design.
DIND_JOB_CGROUP_OOM = "Job memory limit exceeded (all leaves together)"

# `oom_memcg=` is the cgroup whose limit was breached, so its last component is a leaf name only
# when the breach is one of ours - see `dind_leaf_oom_in_dmesg` for what that discriminates.
# The kernel prints the fields comma-separated, hence `[^,\s]` rather than `\S`.
MEMCG_OOM_OWNER = re.compile(rb"oom_memcg=([^,\s]+)")
# The breached cgroup paired with the victim's own, which is what distinguishes a breach ABOVE the
# leaves from a breach OF one. The kernel always prints them adjacent and in this order.
MEMCG_OOM_KILL = re.compile(rb"oom_memcg=([^,\s]+),task_memcg=([^,\s]+)")

MAX_CPUS_PER_WORKER = 5
MAX_MEM_PER_WORKER = 11
# Flaky/targeted checks run with --dist=each, so every worker runs the full set
# of changed modules concurrently (each with its own Docker cluster) instead of
# splitting modules across workers. A worker's peak footprint is therefore much
# larger, so it needs a bigger memory budget to avoid exhausting the container
# cgroup and tripping the kernel OOM killer (see OOM_IN_DMESG_TEST_NAME).
MAX_MEM_PER_WORKER_DIST_EACH = 20

# Bash arithmetic is signed 64-bit, so `[ "$x" -gt 0 ]` errors out above this instead of
# comparing. Measured: 2**63-1 is accepted, 2**63 exits 2 with "integer expression expected".
_SHELL_INT_MAX = 2**63 - 1


def nested_budget_gb(env=None, physical_memory=None) -> int:
    """Memory the nested test containers may collectively use, in GiB.

    `CI_DIND_NESTED_BUDGET` is the cap `docker_in_docker.sh` puts on the cgroup that parents
    them, so deriving worker concurrency from it keeps scheduling and containment on the same
    number. `Utils.physical_memory` reports HOST memory, which those containers do not get:
    on the 61.78 GiB runner it budgets 3 x 20 = 60 GiB for a `--dist=each` run inside a
    40 GiB cap. Runs without the variable (local, or a job that sets no memory limit) keep
    deriving it from host memory, which is what they do today.
    """
    env = os.environ if env is None else env
    physical = Utils.physical_memory if physical_memory is None else physical_memory
    budget = env.get("CI_DIND_NESTED_BUDGET")
    if budget is None:
        return round(physical() // (1024**3), 1)
    # `docker_in_docker.sh`'s contract, which this must not diverge from: ASCII digits only,
    # positive, and no larger than the job limit. Neither `int()` (takes `-1`, ` 5 `, `1_000`)
    # nor `str.isdigit()` (takes non-ASCII digits) is that contract.
    ceiling = env.get("CI_DIND_JOB_MEM") or ""
    ceiling = int(ceiling) if re.fullmatch(r"[0-9]+", ceiling) else _SHELL_INT_MAX
    if not re.fullmatch(r"[0-9]+", budget) or not 0 < int(budget) <= ceiling:
        raise ValueError(
            f"CI_DIND_NESTED_BUDGET is [{budget}], expected a positive byte count no larger "
            f"than the job limit [{ceiling}]; a zero budget means the reserves leave the "
            "test containers nothing, so integration tests need a larger host"
        )
    return round(int(budget) // (1024**3), 1)


def worker_plan(mem_limit_gb: int, cpus: int, dist_each: bool) -> Tuple[int, int]:
    """`(workers, gb_per_worker)` for a `mem_limit_gb` container budget and `cpus`.

    `dist_each` runs every module on every worker, so a worker's peak footprint is larger.

    A job with no workers is useless, so there is always at least one - but on a carrier too
    small to hold even one modeled worker that floor would claim memory the budget does not
    have. Report what such a worker actually gets instead of the model figure, and warn: silently
    assuming a budget that is not there is the defect this sizing exists to remove.
    """
    modeled = MAX_MEM_PER_WORKER_DIST_EACH if dist_each else MAX_MEM_PER_WORKER
    workers = max(min(cpus // MAX_CPUS_PER_WORKER, mem_limit_gb // modeled), 1)
    if workers * modeled > mem_limit_gb:
        print(
            f"WARNING: a worker is modeled at {modeled} GiB but the container budget is "
            f"{mem_limit_gb} GiB; running {workers} worker(s) with less memory than the "
            "integration tests are sized for"
        )
        return workers, max(mem_limit_gb // workers, 0)
    return workers, modeled


def pytest_workers(mem_limit_gb: int, cpus: int, dist_each: bool) -> int:
    """Number of xdist workers to run. See `worker_plan`."""
    return worker_plan(mem_limit_gb, cpus, dist_each)[0]


def planned_workers(
    args_workers: Optional[int], mem_limit_gb: int, cpus: int, dist_each: bool
) -> int:
    """`--workers` when given, else the budget-derived plan. See `worker_plan`."""
    if args_workers:
        return args_workers
    print("ncpu:", cpus)
    print("mem_gb:", mem_limit_gb)
    return pytest_workers(mem_limit_gb, cpus, dist_each=dist_each)


def _cgroup_field(leaf_path: Path, filename: str, field: str) -> Optional[int]:
    """`field`'s value in a `key value` cgroup file, or None if it cannot be read."""
    try:
        for line in (leaf_path / filename).read_text(encoding="utf-8").splitlines():
            key, _, value = line.partition(" ")
            if key == field:
                return int(value)
    except (OSError, ValueError):
        # No containment (the permissive path creates no leaves), the other cgroup version, or a
        # partial read. Absence of evidence is not an error here.
        return None
    return None


def _cgroup_own_limit_ooms(leaf_path: Path) -> int:
    """How many times this cgroup breached its OWN memory cap, or 0 if unreadable.

    `memory.events.local`, not the aggregating `memory.events`: the aggregating form also counts
    a descendant breaching ITS cap, and every test container carries the `mem_limit` its module
    asked for (12g by default, `tests/integration/helpers/cluster.py`), an outcome that predates
    this cap and that some modules tolerate deliberately. Reading it would report those as job
    errors. Measured over 3 alternating pairs: a container exceeding its own cap moves the
    aggregating `oom_kill` and leaves local `oom` at 0; a collective breach of `/docker` moves
    local `oom`.

    The field is `oom` rather than `oom_kill` because it has to be correct for both leaf shapes:
    `/docker` holds the containers as children, so a breach of its cap kills a task charged to a
    child and only `oom` lands here, while `/init` and `/dockerd` hold their processes directly
    and set both.

    v1 has no file with those semantics, so it falls back to `memory.oom_control`'s `oom_kill`,
    which is charged to the KILLED TASK's cgroup. That is exact for the leaves that hold their
    processes directly and structurally 0 for `/docker`, whose containers are children - so it is
    a partial signal, and the dmesg scan remains the general v1 detector. `failcnt` is not usable
    here: measured, it took 775428 increments from reclaimable page cache with 0 kills.
    """
    v2 = _cgroup_field(leaf_path, "memory.events.local", "oom")
    if v2 is not None:
        return v2
    return _cgroup_field(leaf_path, "memory.oom_control", "oom_kill") or 0


def dind_leaf_root(cgroup_root=DIND_CGROUP_ROOT) -> Path:
    """Where `docker_in_docker.sh` puts its leaves: `<root>` on v2, `<root>/memory` on v1.

    v1 mounts the memory controller in its own hierarchy, so a reader that assumes the v2 layout
    finds nothing on the production runners.
    """
    root = Path(cgroup_root)
    if (root / "cgroup.controllers").is_file():
        return root
    return root / "memory" if (root / "memory").is_dir() else root


def leaf_peak_usage(cgroup_root=DIND_CGROUP_ROOT) -> Dict[str, int]:
    """Peak bytes each leaf ever charged, for the leaves that report it.

    The reserves are the only numbers here that cannot be derived, and a leaf that was killed
    reports a peak equal to its own cap: the demand above it is not recorded anywhere, so sizing a
    reserve from a breached run measures the cap rather than the workload. Printing the peak of a
    run that did NOT breach is what makes the next reserve a measurement.

    `memory.peak` is cgroup v2 and needs Linux 5.19. On v1 the cap is applied to memory and to
    memory-plus-swap at the same number, so the memsw peak is the one that can reach it, and the
    resident-only peak can sit below a cap that swap breached. A missing file is a leaf that cannot
    report, never an error.
    """
    leaf_root = dind_leaf_root(cgroup_root)
    peaks = {}
    for leaf in DIND_LEAF_MEANINGS:
        for name in (
            "memory.peak",
            "memory.memsw.max_usage_in_bytes",
            "memory.max_usage_in_bytes",
        ):
            try:
                peaks[leaf] = int(
                    (leaf_root / leaf / name).read_text(encoding="utf-8").strip()
                )
                break
            except (OSError, ValueError):
                continue
    return peaks


def print_leaf_peak_usage(env, cgroup_root=DIND_CGROUP_ROOT) -> Dict[str, int]:
    """Print `leaf_peak_usage` against each leaf's cap, and return what was printed.

    Only under required containment: elsewhere these paths are the host's cgroups.
    """
    if env.get("CI_DIND_REQUIRE_CGROUP_CONTAINMENT") != "1":
        return {}
    peaks = leaf_peak_usage(cgroup_root=cgroup_root)
    caps = {
        # The cap that was written, which for `/init` and `/dockerd` is not their share of the
        # budget. Falling back to the reserve would understate the cap and print a false AT CAP.
        "init": env.get("CI_DIND_INIT_LIMIT") or env.get("CI_DIND_INIT_RESERVE"),
        "dockerd": env.get("CI_DIND_DAEMON_LIMIT") or env.get("CI_DIND_DAEMON_RESERVE"),
        "docker": env.get("CI_DIND_NESTED_BUDGET"),
    }
    for leaf, peak in sorted(peaks.items()):
        cap = caps.get(leaf)
        # A peak at the cap means the leaf was throttled or killed there, so the figure is a
        # lower bound on what the workload wanted rather than its footprint.
        censored = " (AT CAP - demand is at least this)" if cap and peak >= int(cap) else ""
        of_cap = f" of {int(cap) / 1024**3:.2f} GiB cap" if cap else ""
        print(f"cgroup leaf /{leaf}: peak {peak / 1024**3:.2f} GiB{of_cap}{censored}")
    return peaks


def leaf_oom_results(cgroup_root=DIND_CGROUP_ROOT) -> List[Result]:
    """One `ERROR` result per `docker_in_docker.sh` leaf that exhausted its own budget.

    Without this the change would remove the only automated report of the condition it enforces:
    previously such an overrun exhausted host RAM and produced the `CONSTRAINT_NONE` kill the
    dmesg scan catches, whereas now it is confined to a capped leaf, the killed container's
    client reports `Connection reset by peer`, and `_mark_infrastructure_errors` would relabel
    that `SKIPPED` - turning a resource kill into a green job.

    On v1 this covers the leaves that hold their own processes; a collective `/docker` breach is
    only visible in dmesg there, because the counter moves on the killed child and no post-hoc
    reading can tell that kill apart from a container reaching the `mem_limit` its own module
    asked for - by teardown the child cgroup is gone. `dind_unreportable_ooms` names that gap
    rather than leaving it silent.
    """
    leaf_root = dind_leaf_root(cgroup_root)
    results = []
    for leaf, meaning in DIND_LEAF_MEANINGS.items():
        if _cgroup_own_limit_ooms(leaf_root / leaf):
            results.append(Result(name=meaning, status=Result.Status.ERROR))
    return results


def dind_unreportable_ooms(
    env, have_covering_dmesg: bool, cgroup_root=DIND_CGROUP_ROOT
) -> List[str]:
    """The reports this run cannot produce, so a green result does not rule them out.

    v1's counter is charged to the KILLED TASK's cgroup, so a breach whose victim sits in a
    descendant never moves the breached cgroup's own counter. Two cgroups here are breached that
    way: `/docker`, which holds the containers as children, and the job's cgroup, whose leaf caps
    may sum above it. dmesg is the only signal for either, and reporting nothing without it is
    indistinguishable from a clean run - a resource kill that reads as clean is the failure mode
    this whole path exists to remove.

    The argument is that a record SPANS this run, not merely that a dump was produced: a dump of
    a buffer that already wrapped succeeds while holding none of the window a kill would be in,
    and silencing this warning on one leaves exactly the clean-looking kill above.

    One probe answers both, since a cgroup and its child are always the same version.

    Only under required containment: elsewhere `/docker` is the host's own cgroup, and naming a
    gap in reports this run never promised is noise.
    """
    if env.get("CI_DIND_REQUIRE_CGROUP_CONTAINMENT") != "1":
        return []
    if have_covering_dmesg:
        return []
    docker = dind_leaf_root(cgroup_root) / "docker"
    on_v2 = _cgroup_field(docker, "memory.events.local", "oom") is not None
    exists = on_v2 or _cgroup_field(docker, "memory.oom_control", "oom_kill") is not None
    if on_v2 or not exists:
        return []
    return [DIND_LEAF_MEANINGS["docker"], DIND_JOB_CGROUP_OOM]


def dind_leaf_oom_in_dmesg(dmesg: bytes, leaves=tuple(DIND_LEAF_MEANINGS)) -> Set[str]:
    """Names of the leaves dmesg records a cgroup OOM against.

    Backstop for `leaf_oom_results`, which reads counters that a mid-run daemon restart or a
    recreated leaf would reset. Scoped to the breached cgroup because an unscoped
    `CONSTRAINT_MEMCG` match would also fire on a test container reaching its own `mem_limit`:
    measured, the two differ only in `oom_memcg=`, which ends at the leaf for a collective
    breach and at a per-container cgroup otherwise.
    """
    return {
        name
        for owner in MEMCG_OOM_OWNER.findall(dmesg)
        if (name := owner.rsplit(b"/", 1)[-1].decode(errors="replace")) in leaves
    }


def job_cgroup_oom(dmesg: bytes, cgroup_root=DIND_CGROUP_ROOT) -> bool:
    """Whether the job's own cgroup, rather than one of its leaves, breached its cap.

    Under `--cgroupns=private` the namespace root IS the job's cgroup, so its own counter answers
    this. The dmesg arm covers a counter a mid-run daemon restart reset, and also the v1 case the
    counter cannot see at all: there the fallback counts victims of the cgroup they belong to, and
    a parent breach usually kills a task belonging to a child.

    The job's cgroup is identified by the one property that holds under every runtime's naming -
    it is the parent of the leaves this script created - rather than by a path shape, since
    production reports `/docker/<id>/init` while a systemd-scoped host reports
    `/system.slice/docker-<id>.scope/init`.
    """
    if _cgroup_own_limit_ooms(dind_leaf_root(cgroup_root)):
        return True
    leaves = tuple(name.encode() for name in DIND_LEAF_MEANINGS)
    for owner, task in MEMCG_OOM_KILL.findall(dmesg):
        if not task.startswith(owner + b"/"):
            continue
        # The victim is below the breached cgroup, so name the first component between them: a
        # leaf there means the breach is ABOVE every leaf and is the job's own. A leaf breach puts
        # a container id there, and a container's own kill has owner == task, excluded above.
        if task[len(owner) + 1 :].split(b"/", 1)[0] in leaves:
            return True
    return False


def leaf_oom_report(
    env, dmesg: bytes, cgroup_root=DIND_CGROUP_ROOT
) -> Tuple[List[Result], bool]:
    """`(results, attach_dmesg)` for a required-containment run; `([], False)` otherwise.

    An exhausted leaf is what an overrun looks like now that the leaves are capped, and it must
    not be swallowed: the killed container's client raises `Connection reset by peer`, which
    `_mark_infrastructure_errors` would relabel `SKIPPED`, so a run the cap killed could finish
    green.

    Only looks when containment was requested: otherwise these paths are the HOST's cgroups.

    `dmesg` must hold only this run's records: leaf names repeat across the jobs a runner hosts,
    so an inherited buffer would attribute another job's kill to this one. Callers pass `b""`
    when they cannot establish that, which leaves the counters as the sole source.
    """
    if env.get("CI_DIND_REQUIRE_CGROUP_CONTAINMENT") != "1":
        return [], False
    results = leaf_oom_results(cgroup_root=cgroup_root)
    # A leaf whose counter fired and which also appears in dmesg must yield one row.
    reported = {
        leaf
        for leaf, meaning in DIND_LEAF_MEANINGS.items()
        if any(r.name == meaning for r in results)
    }
    for leaf in sorted(dind_leaf_oom_in_dmesg(dmesg) - reported):
        meaning = DIND_LEAF_MEANINGS[leaf]
        results.append(Result(name=meaning, status=Result.Status.ERROR))
    if job_cgroup_oom(dmesg, cgroup_root=cgroup_root):
        results.append(Result(name=DIND_JOB_CGROUP_OOM, status=Result.Status.ERROR))
    # Attached once rather than per leaf: `create_from` stores `files` verbatim, so three
    # breached leaves plus a host OOM would list the same path four times.
    return results, bool(results)


def prefetch_failure_result(env=None, cgroup_root=DIND_CGROUP_ROOT) -> Result:
    """The image pre-pull failure, carrying whatever the leaves say about why.

    This path ends the job through `complete_job`, so the end-of-run leaf scan never runs on it.
    A daemon its own cap killed presents here as a pull failure, and without these rows the job
    reports only an unexplained infrastructure error.

    `b""` rather than a dmesg dump: this runs before the buffer is cleared, and leaf names repeat
    across the jobs a runner hosts, so a dump read here could name a previous job's kill. The
    counters are charged to this run's own cgroups and need no dmesg.
    """
    env = os.environ if env is None else env
    print_leaf_peak_usage(env, cgroup_root=cgroup_root)
    results, _ = leaf_oom_report(env, b"", cgroup_root=cgroup_root)
    return Result.create_from(
        status=Result.Status.ERROR,
        info="Failed to pre-pull Docker images needed by the test batch",
        results=results,
        files=[DOCKER_IN_DOCKER_LOG] if Path(DOCKER_IN_DOCKER_LOG).exists() else None,
        labels=[Result.Label.INFRA],
    )


def report_late_leaf_ooms(
    R: Result,
    env,
    dmesg: bytes,
    already_reported: Set[str],
    lost_coverage_artifact: bool = False,
    cgroup_root=DIND_CGROUP_ROOT,
) -> List[Result]:
    """Append every leaf that breached only after the first scan to `R`, and escalate it.

    The first scan runs before the last `/init` work - the coverage shards' `llvm-profdata
    merge` - and a merge the cap kills is only printed, never raised, so without this the shard
    finishes green minus its artifact. The counters are cumulative and the reread is idempotent,
    hence `already_reported`: one breach yields one row however many scans see it.

    `lost_coverage_artifact` only names a likely consequence of a breach that happened; a merge
    that failed on its own is pre-existing behavior and stays out of the status.

    Writes `info`, so it owns the failure summary on the runs it fires, the same contract
    `report_rabbitmq_recreations` follows.
    """
    rows = [
        r
        for r in leaf_oom_report(env, dmesg, cgroup_root=cgroup_root)[0]
        if r.name not in already_reported
    ]
    if not rows:
        return []
    R.results.extend(rows)
    R.set_error()
    # `_add_job_summary_to_info` writes `Failures: N/M` only while `info` is empty, so the
    # write below would delete the count. Emitted here, after the rows, so it counts them.
    if not R.info:
        fail_cnt = sum(1 for r in R.results if not r.is_ok())
        R.set_info(f"Failures: {fail_cnt}/{len(R.results)}")
    for row in rows:
        R.set_info(f"{row.name} - infrastructure/resource failure, not bug reproduction")
    if lost_coverage_artifact:
        R.set_info("No LLVM coverage artifact was produced, consistent with that kill")
    return rows


def start_dmesg_follow() -> Optional[subprocess.Popen]:
    """Capture kernel messages from now on, or `None` when following was refused.

    The clear-to-here window needs no marker: `--follow` prints the buffer it starts with
    before following, so anything logged in between is still there and is captured.

    The argv is a list, not a shell string: a shell in between would make `poll` report the
    shell rather than `dmesg`, and `poll` is what coverage rests on. `sudo` forks a child of
    its own regardless, hence the new session, so both can be signalled as one group.

    A refusal that takes longer to surface than the delay below is not lost: coverage is decided
    by polling this process again where the record is read, not by what is returned here.
    """
    with open(DMESG_FOLLOW_LOG, "w") as log_file:
        proc = subprocess.Popen(
            ["sudo", "dmesg", "-T", "--follow"],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    # `Popen` reports only that the fork happened, so a `sudo` or `dmesg` that exits at once is
    # still running here. `start_docker_in_docker` waits the same way before trusting its daemon.
    time.sleep(1)
    if proc.poll() is not None:
        print(
            f"WARNING: could not follow dmesg (rc={proc.returncode}); the kernel record will "
            "cover only what the ring buffer still holds at the end of the run"
        )
        return None
    print(f"Following dmesg into {DMESG_FOLLOW_LOG} with PID {proc.pid}")
    return proc


def read_dmesg_follow() -> bytes:
    """Everything the follower has captured so far, whole.

    Unwindowed at every read point: `report_late_leaf_ooms` dedupes by `already_reported`, so a
    cumulative buffer yields one row per breach however many scans see it.

    The final line can be torn mid-write. Harmless: every consumer matches whole tokens, so a
    torn line fails to match, and the same line reappears complete in the terminal snapshot.
    """
    if not Path(DMESG_FOLLOW_LOG).exists():
        return b""
    with open(DMESG_FOLLOW_LOG, "rb") as follow_file:
        return follow_file.read()


def stop_dmesg_follow(proc: Optional[subprocess.Popen]) -> None:
    """Stop the follower.

    `Utils.terminate_process_group` neither waits nor reports the outcome, so the wait is what
    makes the stop observable and what keeps the child from being left unreaped. `SIGKILL` needs
    no second wait to confirm it, and this runs one statement before the job's report is written,
    where waiting again on a child that already ignored `SIGTERM` would cost the run that report.
    """
    if proc is None or proc.poll() is not None:
        # Signalling a group whose leader is already reaped only logs an ESRCH error.
        return
    Utils.terminate_process_group(proc.pid)
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        print("WARNING: dmesg follower ignored SIGTERM; killing it")
        Utils.terminate_process_group(proc.pid, force=True)


def print_oom_lines(dmesg: str, caveat: str = "", partial: str = "") -> None:
    """Print the kernel's memory-kill lines, whichever scope each one happened in.

    Read without attribution: the point is to say whether the kernel killed anything at all,
    which the leaf counters cannot answer for a kill in a descendant. Unattributable is not the
    same as absent, so no result row is derived from it here - `leaf_oom_report` owns that, on a
    buffer it can scope. An empty buffer is a third outcome and not "no kill", so it says so.

    The two caveats ride opposite branches, because a buffer is unsound in opposite directions.
    `caveat` rides the kills, so a reader cannot take another job's for this run's. `partial`
    rides the absence, which a record that does not span the run cannot establish.
    `OOM_DMESG_MARKERS` are `str`: a bytes caller decodes.
    """
    if not dmesg:
        print("WARNING: no dmesg available, so a kernel kill can neither be shown nor ruled out")
        return
    if oom_lines := [l for l in dmesg.splitlines() if any(m in l for m in OOM_DMESG_MARKERS)]:
        print(f"Kernel memory kills in dmesg{caveat}:")
        for line in oom_lines:
            print(f"  {line}")
    else:
        print(f"No kernel memory kill in dmesg{partial}")


def print_timeout_diagnostics(
    env, follow_proc=None, dmesg_cleared=False, cgroup_root=DIND_CGROUP_ROOT
) -> None:
    """Print what a run killed by the time budget was doing, to stdout.

    Everything else on this path is an uploaded artifact, and an upload needs the job to survive
    long enough to make one. A run that hit the backstop is already past its budget, so the runner
    cancels it while the archive is still being written and nothing is uploaded at all - measured
    on job 95139621296, where the archive got 13.3 s. The job LOG is the one channel that is kept
    regardless, so the small diagnostics go there and go first.

    dmesg is read here rather than reused: this path runs before the end-of-run dump. The
    follower's record is read with it, since the snapshot alone reaches back only as far as the
    ring buffer still holds.

    Both defaults are UNCOVERED and UNCLEARED, because this runs on every non-local hard timeout
    whatever the clear did. The file's existence cannot stand in for the follower, either - one
    that died early leaves a file that begins right where a healthy one would.

    An empty snapshot is read as a failed one, since `Shell.get_output` returns `""` for both a
    failure and a genuinely empty buffer. That conflation over-warns, which is the safe direction.
    """
    print_leaf_peak_usage(env, cgroup_root=cgroup_root)
    follow_dmesg = read_dmesg_follow().decode(errors="replace")
    snapshot = Shell.get_output("dmesg -T", verbose=True)
    covers_run = follow_proc is not None and follow_proc.poll() is None and bool(snapshot)
    print_oom_lines(
        follow_dmesg + snapshot,
        caveat="" if dmesg_cleared else UNCLEARED_DMESG_CAVEAT,
        partial="" if covers_run else PARTIAL_DMESG_CAVEAT,
    )


ncpu = Utils.cpu_count()

# A timeout says nothing about its own origin: a container orchestration command and
# the process under test both raise `subprocess.TimeoutExpired`, rendering the same two
# substrings. These two are therefore matched by the argv that timed out rather than by
# a plain substring search.
TIMEOUT_ERROR_PATTERNS = [
    "timed out after",
    "TimeoutExpired",
]

INFRASTRUCTURE_ERROR_PATTERNS = TIMEOUT_ERROR_PATTERNS + [
    "Cannot connect to the Docker daemon",
    "Error response from daemon",
    "Name or service not known",
    "Temporary failure in name resolution",
    "Network is unreachable",
    "Connection reset by peer",
    "No space left on device",
    "Cannot allocate memory",
    "OCI runtime create failed",
    "toomanyrequests",
    "pull access denied",
    "Got exception pulling images:",  # docker pull failure during cluster.start()
]

# compose options that consume the token after them, so the subcommand is not the
# first non-option token but the first one no option has claimed.
COMPOSE_VALUED_OPTIONS = {
    "--ansi",
    "--env-file",
    "--file",
    "--parallel",
    "--profile",
    "--progress",
    "--project-directory",
    "--project-name",
    "-f",
    "-p",
}

# Subcommands whose timeout does not mean the server failed to respond, so exceeding the
# python-side budget says nothing about the server under test.
ORCHESTRATION_LIFECYCLE_VERBS = {
    "config",
    "create",
    "down",
    "images",
    "login",
    "logs",
    "ps",
    "pull",
    "rm",
    "start",
    "unpause",
    "up",
}

# Subcommands that wait on the server exiting: an unguarded `stop` with no `--timeout`
# (cluster.py:2641) is bounded only by the generated template's `stop_grace_period: 10m`,
# which outlives the python-side budget, so its timeout means the server did not respond.
ORCHESTRATION_PRODUCT_VERBS = {"kill", "pause", "restart", "stop"}

# Top-level `docker` subcommands the harness runs on its own behalf, outside compose, and
# whose timeout is known to arrive here (`run_and_check` re-raises `TimeoutExpired` even
# under `nothrow`). `exec` and `update` are absent: a test body runs those.
DOCKER_TOPLEVEL_LIFECYCLE_VERBS = {"login", "ps", "rm"}


def _raising_exception_lines(info: str) -> list:
    """The `E   <ExcType>: <msg>` lines, i.e. the exceptions actually raised.

    Scoped to these lines because an embedded server stack trace can carry a timeout
    substring tens of kilobytes away from anything that timed out.
    """
    return [line for line in info.splitlines() if line.startswith("E ")]


def _argv_lists(line: str) -> list:
    """Every bracketed argv on `line`, in either rendering."""
    argvs = []
    for match in re.finditer(r"\[((?:'[^']*'(?:,\s*)?)+)\]", line):
        argvs.append(re.findall(r"'([^']*)'", match.group(1)))
    for match in re.finditer(r"\[([^\[\]']+)\]", line):
        argvs.append(match.group(1).split())
    return argvs


def _orchestration_verb(argv: list):
    """The docker subcommand `argv` invokes, or None if it is not orchestration."""
    if len(argv) < 2 or argv[0] != "docker":
        return None
    if argv[1] in DOCKER_TOPLEVEL_LIFECYCLE_VERBS:
        return argv[1]
    if argv[1] != "compose":
        return None
    i = 2
    while i < len(argv):
        if argv[i] in COMPOSE_VALUED_OPTIONS:
            i += 2
        elif argv[i].startswith("-"):
            i += 1
        else:
            return argv[i]
    return None


def _is_orchestration_lifecycle_timeout(info: str) -> bool:
    """Whether the timeout is docker's or the registry's rather than the server's.

    Every command a raised exception reports as timing out must be one docker runs on the
    harness's behalf. A row can name several: `raise ... from ex` makes pytest render both
    exceptions with their own `E ` prefix, a teardown reports its own commands beside the
    body's, and captured output is embedded verbatim in the message. So anything else on a
    timeout-bearing line -- a product-sensitive subcommand, a command that is not
    orchestration at all, or no argv whatsoever -- means the wait that expired is not
    known to be docker's.

    An unrecognised subcommand is not a lifecycle one: a new compose verb must be
    classified deliberately rather than default to suppressing the result.
    """
    saw_lifecycle = False
    for line in _raising_exception_lines(info):
        if not any(p in line for p in TIMEOUT_ERROR_PATTERNS):
            continue
        argvs = _argv_lists(line)
        if not argvs:
            return False
        for argv in argvs:
            verb = _orchestration_verb(argv)
            if verb is None or verb in ORCHESTRATION_PRODUCT_VERBS:
                return False
            if verb not in ORCHESTRATION_LIFECYCLE_VERBS:
                return False
            saw_lifecycle = True
    return saw_lifecycle


def _non_timeout_patterns_match(info: str) -> bool:
    return any(
        p in info for p in INFRASTRUCTURE_ERROR_PATTERNS
        if p not in TIMEOUT_ERROR_PATTERNS
    )


def _is_infrastructure_error(result: Result) -> bool:
    """Returns True if the result is a failure caused by infrastructure issues."""
    if not result.info:
        return False
    if result.status == Result.Status.ERROR:
        return _non_timeout_patterns_match(
            result.info
        ) or _is_orchestration_lifecycle_timeout(result.info)
    # Docker compose/pull infrastructure failures may appear with FAIL status
    # when pytest reports fixture (setup phase) errors as test failures.
    # Require both docker context and an infrastructure pattern to avoid
    # false positives on genuine test failures.
    if result.status == Result.Status.FAIL:
        has_docker_context = (
            "'docker'" in result.info or "images_pull_cmd" in result.info
        )
        return has_docker_context and (
            _non_timeout_patterns_match(result.info)
            or _is_orchestration_lifecycle_timeout(result.info)
        )
    return False


def _mark_infrastructure_errors(results: list) -> int:
    """Scan results, label infrastructure errors with INFRA and change their status to SKIPPED.

    Returns the number of results that were relabeled.
    """
    count = 0
    for r in results:
        if _is_infrastructure_error(r):
            r.set_label(Result.Label.INFRA)
            r.status = Result.Status.SKIPPED
            count += 1
    if count:
        print(f"Marked {count} test result(s) as infrastructure errors")
    return count


def clear_rabbitmq_recreation_scan_inputs() -> None:
    """Delete everything `report_rabbitmq_recreations` scans, before the first batch.

    The reporter scans every line and the log handlers append, so anything left in
    `temp_path` by an earlier job counts as an event of this one. Best effort: a job
    must never fail because a stale file could not be removed.
    """
    try:
        stale_files = sorted(Path(temp_path).glob("pytest_*.log")) + sorted(
            Path(temp_path).glob("rabbit-*.log")
        )
    except OSError as ex:
        print(f"WARNING: cannot list {temp_path} before RabbitMQ retry scan: {ex}")
        return
    for stale in stale_files:
        try:
            os.remove(stale)
        except OSError as ex:
            print(f"WARNING: cannot remove {stale} before RabbitMQ retry scan: {ex}")


def report_rabbitmq_recreations(result: Result) -> int:
    """Publish RabbitMQ container recreations, and the broker logs the waiter preserved.

    Must be called once per job, after every batch: the per-worker log handlers append
    and a sequential batch reuses one log file across repeats, so scanning per batch
    would report a multiple of the real count.
    """
    snapshots = []
    count = 0
    for log_file in sorted(Path(temp_path).glob("pytest_*.log")):
        # Streamed: these are the full per-worker integration logs, tens of MB each.
        # A file contributes only once read to the end, so a partial read is no count.
        file_count = 0
        file_snapshots = []
        try:
            with log_file.open(encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if RABBITMQ_RECREATE_TOKEN not in line:
                        continue
                    file_count += 1
                    # The field is a bare file name in `temp_path`: the whitespace-
                    # delimited parse below cannot carry a directory, which may contain
                    # spaces.
                    match = re.search(r"snapshot=(\S+)", line)
                    if not match or os.path.basename(match.group(1)) != match.group(1):
                        continue
                    snapshot = os.path.join(temp_path, match.group(1))
                    if os.path.isfile(snapshot):
                        file_snapshots.append(snapshot)
        except OSError as ex:
            print(f"WARNING: cannot read {log_file} for RabbitMQ retry scan: {ex}")
            continue
        count += file_count
        snapshots.extend(file_snapshots)
    if not count:
        return 0
    # Only `info` and `files` are touched; status and labels stay as the results left them.
    # `complete_job` appends `Failures: N/M` only while `info` is empty and runs after
    # this, so emit it here on exactly the runs that would otherwise have received it.
    if not result.info:
        fail_cnt = sum(1 for r in result.results if not r.is_ok())
        result.set_info(f"Failures: {fail_cnt}/{len(result.results)}")
    result.set_info(
        f"RabbitMQ container recreation was attempted {count} time(s)"
        " after failing to start"
    )
    for snapshot in snapshots:
        if snapshot not in result.files:
            result.files.append(snapshot)
    print(f"NOTE: RabbitMQ container recreations observed: {count}")
    return count


def quote_tests(tests: List[str]) -> str:
    """Join test node IDs into a shell-safe, space-separated string.

    A parametrized integration test node ID can contain spaces, parentheses and
    quotes when the test is parametrized with SQL (e.g.
    `test.py::test_simple_append[SELECT now() FROM numbers(2)]`). The pytest
    command is executed through a shell, so each node ID must be quoted to
    survive as a single argument instead of being split or mis-parsed.
    """
    return " ".join(shlex.quote(t) for t in tests)


def start_docker_in_docker():
    with open(DOCKER_IN_DOCKER_LOG, "w") as log_file:
        dockerd_proc = subprocess.Popen(
            "./ci/jobs/scripts/docker_in_docker.sh",
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
    retries = 20
    for i in range(retries):
        # On last retry, show errors; otherwise suppress them
        cmd = "docker info > /dev/null" if i == retries - 1 else "docker info > /dev/null 2>&1"
        if Shell.check(cmd, verbose=True):
            break
        if dockerd_proc.poll() is not None:
            # The script exited instead of starting the daemon, e.g. it refused because the
            # cgroup containment it was asked for could not be established. Say so now rather
            # than after 40 more seconds of a timeout that names the wrong cause.
            raise RuntimeError(
                f"docker_in_docker.sh exited early (rc={dockerd_proc.returncode}); "
                "see ./ci/tmp/docker-in-docker.log"
            )
        if i == retries - 1:
            raise RuntimeError(
                f"Docker daemon didn't responded after {retries} attempts"
            )
        time.sleep(2)
    print(f"Started docker-in-docker asynchronously with PID {dockerd_proc.pid}")


_COMPOSE_DIR = Path("./tests/integration/compose")

# Explicit mapping for with_* flags whose compose file name cannot be derived
# by simply prepending "docker_compose_" and appending ".yml".
_WITH_FLAG_TO_COMPOSE: dict[str, List[str]] = {
    "mysql57": ["docker_compose_mysql.yml"],
    "mysql8": ["docker_compose_mysql_8_0.yml"],
    "dremio26": ["docker_compose_dremio_26_0.yml"],
    "kerberos_kdc": ["docker_compose_kerberos_kdc.yml"],
    # with_iceberg_catalog can use any of the iceberg catalogs; include them all
    "iceberg_catalog": [
        "docker_compose_iceberg_rest_catalog.yml",
        "docker_compose_iceberg_hms_catalog.yml",
        "docker_compose_iceberg_lakekeeper_catalog.yml",
        "docker_compose_iceberg_nessie_catalog.yml",
        "docker_compose_iceberg_seaweedfs_catalog.yml",
    ],
    "hms_catalog": ["docker_compose_iceberg_hms_catalog.yml"],
    "glue_catalog": ["docker_compose_glue_catalog.yml"],
    "prometheus_writer": ["docker_compose_prometheus_writer.yml"],
    "prometheus_reader": ["docker_compose_prometheus_reader.yml"],
    "prometheus_receiver": ["docker_compose_prometheus_receiver.yml"],
    # with_odbc_drivers implicitly sets up mysql8 + postgres
    "odbc_drivers": ["docker_compose_mysql_8_0.yml", "docker_compose_postgres.yml"],
    # Flags with no separate compose file of their own
    "jdbc_bridge": [],
    "net_trics": [],
}


def get_compose_files_for_test_modules(test_modules: List[str]) -> List[Path]:
    """Return compose files needed by the given test modules.

    Grep every Python source file in each test suite directory for:
    - `with_X=True` patterns (mapped via `_WITH_FLAG_TO_COMPOSE` or the obvious
      `docker_compose_{X}.yml` naming convention), and
    - explicit `docker_compose_*.yml` file name strings (used e.g. via
      `extra_parameters={"docker_compose_file_name": "..."}` calls).
    """
    needed: set[Path] = set()
    suite_dirs = {m.split("/")[0] for m in test_modules}

    for suite_dir in suite_dirs:
        suite_path = Path("./tests/integration/") / suite_dir
        if not suite_path.is_dir():
            continue
        for py_file in suite_path.glob("**/*.py"):
            try:
                content = py_file.read_text(errors="replace")
            except OSError:
                continue

            # 1. with_X=True → compose file via mapping or naming convention
            for m in re.finditer(r"\bwith_(\w+)\s*=\s*True", content):
                flag = m.group(1)
                if flag in _WITH_FLAG_TO_COMPOSE:
                    for fname in _WITH_FLAG_TO_COMPOSE[flag]:
                        p = _COMPOSE_DIR / fname
                        if p.exists():
                            needed.add(p)
                else:
                    p = _COMPOSE_DIR / f"docker_compose_{flag}.yml"
                    if p.exists():
                        needed.add(p)

            # 2. Directly named compose files (e.g. in extra_parameters dicts)
            for m in re.finditer(r"(docker_compose_\w+\.yml)", content):
                p = _COMPOSE_DIR / m.group(1)
                if p.exists():
                    needed.add(p)

    return sorted(needed)


def get_images_from_compose_files(compose_files: List[Path]) -> List[str]:
    """Parse compose files and return a deduplicated list of image references.

    Environment variable placeholders like `${DOCKER_NGINX_DAV_TAG:-latest}` are
    resolved from `os.environ`.  For clickhouse images that appear without a tag
    (e.g. `clickhouse/integration-test`) the tag is looked up from `IMAGES_ENV`.
    Images with still-unresolvable variables are silently skipped.
    """
    known_image_tags: dict[str, str] = {}
    for image_name, env_var in IMAGES_ENV.items():
        tag = os.environ.get(env_var)
        if tag:
            known_image_tags[image_name] = tag

    def resolve_image(raw: str) -> Optional[str]:
        def replace_var(m: re.Match) -> str:
            var_name = m.group(1)
            default = m.group(2) if m.group(2) is not None else "latest"
            return os.environ.get(var_name, default)

        resolved = re.sub(r"\$\{(\w+)(?::-([^}]*))?\}", replace_var, raw)
        if "${" in resolved:
            return None  # Still-unresolvable variable — skip
        # Append the correct tag for tagless known clickhouse images
        if ":" not in resolved and resolved in known_image_tags:
            resolved = f"{resolved}:{known_image_tags[resolved]}"
        return resolved

    images: set[str] = set()
    for compose_file in compose_files:
        try:
            content = compose_file.read_text()
        except OSError:
            continue
        for m in re.finditer(r"^\s+image:\s+(.+)$", content, re.MULTILINE):
            # Strip inline YAML comments from unquoted values before resolving
            # (e.g. `coredns/coredns:1.9.3 # :latest broke this test`).
            raw = re.sub(r"\s+#.*$", "", m.group(1).strip())
            resolved = resolve_image(raw)
            if resolved:
                images.add(resolved)

    return sorted(images)


def prefetch_images(
    images: List[str],
    retries: int = 3,
    pull_timeout: int = 300,
    parallel: int = PREFETCH_PARALLEL_PULLS,
) -> bool:
    """Pull the images using `ci/prefetch-integration-test-images`.

    Images with no manifest for the current architecture (e.g. amd64-only images
    on arm64 runners) are silently skipped.  Returns True on success, False if any
    image fails to pull for a real reason.
    """
    if not images:
        print("No images to pre-fetch.")
        return True

    script = f"{repo_dir}/ci/jobs/scripts/prefetch-integration-test-images"
    env = {
        **os.environ,
        "PULL_RETRIES": str(retries),
        "PULL_TIMEOUT": str(pull_timeout),
        "PULL_PARALLEL": str(parallel),
    }
    return Shell.check(
        f"{script} {' '.join(images)}",
        verbose=True,
        env=env,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument("--options", help="Job parameters: ...")
    parser.add_argument(
        "--test",
        help="Optional. Test name patterns (space-separated)",
        default=[],
        nargs="+",
        action="extend",
    )
    parser.add_argument(
        "--count",
        help="Optional. Number of times to repeat each test",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--debug",
        help="Optional. Open python debug console on exception",
        default=False,
        action="store_true",
    )
    parser.add_argument(
        "--path",
        help="Optional. Path to custom clickhouse binary",
        type=str,
        default="",
    )
    parser.add_argument(
        "--path_1",
        help="Optional. Path to custom server config",
        type=str,
        default="",
    )
    parser.add_argument(
        "--workers",
        help="Optional. Number of parallel workers for pytest",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--session-timeout",
        help="Optional. Session timeout in seconds",
        default=None,
        type=int,
    )
    parser.add_argument(
        "--param",
        help=(
            "Optional. Comma-separated KEY=VALUE pairs to inject as environment "
            "variables for pytest (e.g. --param PYTEST_ADDOPTS=-vv,CUSTOM_FLAG=1)"
        ),
        type=str,
        default="",
    )
    return parser.parse_args()


def merge_profraw_files(llvm_profdata_cmd: str, run_complete: bool):
    """Merge all profraw files into final profdata file.

    Args:
        llvm_profdata_cmd: Path to llvm-profdata tool
        run_complete: whether the test run executed everything it planned
    """
    import subprocess
    from pathlib import Path

    # Name the profile after this job's own coverage artifact, so the
    # aggregation can tell which shards arrived from the filenames alone.
    # JOB_CONFIG has been through dump()/get() by the time a job body runs, so
    # it is a plain dict here.
    provides = (Info().job_config or {}).get("provides")
    assert (
        isinstance(provides, list)
        and len(provides) == 1
        and isinstance(provides[0], str)
        and provides[0]
    ), f"expected exactly one provided artifact name, got {provides!r}"
    final_file = f"./{provides[0]}.profdata"

    # llvm-profdata truncates its -o target in place instead of replacing it,
    # so a stale profile at the target name must be removed before deciding
    # whether to merge at all - otherwise a skipped or failed merge would let
    # the uploader publish the stale file as this shard's contribution.
    if os.path.exists(final_file):
        print(f"Removing pre-existing {final_file}", flush=True)
        os.unlink(final_file)

    # An incomplete run's .profraw files understate coverage, so publish nothing
    # and let the aggregate job abstain; the inputs stay on disk for inspection.
    if not run_complete:
        print(
            "ERROR: the run timed out or hit an infrastructure error, so this "
            "shard's coverage is incomplete; publishing no profile",
            flush=True,
        )
        return None

    # Find all profraw files
    profraw_files = [str(p) for p in Path(".").rglob("*.profraw")]

    if not profraw_files:
        print("No profraw files found", flush=True)
        return

    # A zero-length .profraw is silently accepted by llvm-profdata at every
    # --failure-mode, so it would drop one process's coverage with no signal.
    # Treat it as an incomplete shard and publish no profile.
    empty_files = [f for f in profraw_files if os.path.getsize(f) == 0]
    if empty_files:
        print(
            f"ERROR: {len(empty_files)} .profraw files are empty, so this shard's coverage "
            f"is incomplete; publishing no profile: {', '.join(empty_files)}",
            flush=True,
        )
        return None

    print(f"Merging {len(profraw_files)} profraw files into {final_file}", flush=True)

    # --failure-mode=any makes the merge all-or-nothing: on any invalid input it
    # exits non-zero and writes no file, so the shard is simply absent (and the
    # aggregate job reports SKIPPED with the shard name) instead of contributing
    # a silently short profile.
    result = subprocess.run(
        [llvm_profdata_cmd, "merge", "-sparse", "-failure-mode=any"]
        + profraw_files
        + ["-o", final_file],
        capture_output=True,
        text=True,
    )

    if result.returncode == 0:
        print(f"Successfully created final coverage file: {final_file}", flush=True)

        # Delete merged profraw files to save disk space
        deleted_count = 0
        for profraw_file in profraw_files:
            try:
                Path(profraw_file).unlink()
                deleted_count += 1
            except Exception as e:
                print(f"  WARNING: Failed to delete {profraw_file}: {e}", flush=True)
        print(f"  Deleted {deleted_count} profraw files", flush=True)
        return final_file
    else:
        print("ERROR: Failed to create final coverage file", flush=True)
        if result.stderr:
            print(result.stderr, flush=True)
        return None




def get_parallel_sequential_tests_to_run(
    batch_num: int,
    total_batches: int,
    args_test: List[str],
    workers: int,
    job_options: str,
    info: Info,
    no_strict: bool = False,
) -> Tuple[List[str], List[str]]:
    if args_test:
        batch_num = 1
        total_batches = 1

    test_files = [
        str(p.relative_to("./tests/integration/"))
        for p in Path("./tests/integration/").glob("test_*/test*.py")
    ]

    if "amd_llvm_coverage" in (job_options or ""):
        before = len(test_files)
        test_files = [
            f
            for f in test_files
            if not any(f.startswith(prefix) for prefix in LLVM_COVERAGE_SKIP_PREFIXES)
        ]
        print(
            f"LLVM coverage: skipped {before - len(test_files)} test files matching LLVM_COVERAGE_SKIP_PREFIXES"
        )

    assert len(test_files) > 100

    parallel_test_modules, sequential_test_modules = get_optimal_test_batch(
        test_files, total_batches, batch_num, workers, job_options, info
    )

    if "excluded_from_llvm" in (job_options or ""):
        excluded_from_llvm_set = {
            f
            for f in (parallel_test_modules + sequential_test_modules)
            if any(f.startswith(prefix) for prefix in LLVM_COVERAGE_SKIP_PREFIXES)
            or "is_built_with_llvm_coverage" in Path(f"./tests/integration/{f}").read_text()
        }
        parallel_test_modules = [f for f in parallel_test_modules if f in excluded_from_llvm_set]
        sequential_test_modules = [f for f in sequential_test_modules if f in excluded_from_llvm_set]
        print(
            f"LLVM coverage disabled-only: kept {len(parallel_test_modules)} parallel and "
            f"{len(sequential_test_modules)} sequential test files "
            f"(from LLVM_COVERAGE_SKIP_PREFIXES or containing is_built_with_llvm_coverage)"
        )

    if not args_test:
        return parallel_test_modules, sequential_test_modules

    # there are following possible values for args.test:
    # 1) test suit (e.g. test_directory or test_directory/)
    # 2) test module (e.g. test_directory/test_module or test_directory/test_module.py)
    # 3) test case (e.g. test_directory/test_module.py::test_case or test_directory/test_module::test_case[test_param])
    def normalize_test_path(test_arg: str) -> str:
        """Normalize test path by removing integration test directory prefixes."""
        # Handle: tests/integration/, integration/, ./tests/integration/, or full paths
        if "tests/integration/" in test_arg:
            # Extract everything after tests/integration/
            test_arg = test_arg.split("tests/integration/", 1)[1]
        elif test_arg.startswith("integration/"):
            # Handle integration/ prefix
            test_arg = test_arg[len("integration/"):]
        return test_arg

    def test_match(test_file: str, test_arg: str) -> bool:
        if "/" not in test_arg:
            return f"{test_arg}/" in test_file
        if test_arg.endswith(".py"):
            return test_file == test_arg
        parts = test_arg.split("::", maxsplit=1)
        test_module = parts[0]
        if test_file.removesuffix(".py") != test_module.removesuffix(".py"):
            return False
        # When a specific test function is requested, verify it exists in the
        # file.  Targeted CI runs pull test names from CIDB, but the test may
        # have been moved or removed since the record was written.  Passing a
        # stale nodeID to pytest causes the entire collection to fail with
        # exit-code 5 ("no tests collected"), aborting all other tests too.
        if len(parts) > 1:
            test_func = parts[1].split("[")[0]  # strip parametrization
            file_path = Path("./tests/integration/") / test_file
            try:
                content = file_path.read_text()
                if f"def {test_func}(" not in content:
                    print(
                        f"WARNING: test function '{test_func}' not found in {test_file}, skipping stale target"
                    )
                    return False
            except OSError:
                return False
        return True

    parallel_tests = []
    sequential_tests = []
    for test_arg in args_test:
        # Normalize the test path first
        normalized_test_arg = normalize_test_path(test_arg)
        matched = False
        for test_file in parallel_test_modules:
            if test_match(test_file, normalized_test_arg):
                parallel_tests.append(normalized_test_arg)
                matched = True
        for test_file in sequential_test_modules:
            if test_match(test_file, normalized_test_arg):
                sequential_tests.append(normalized_test_arg)
                matched = True
        if not no_strict:
            assert matched, f"Test [{test_arg}] not found"

    return parallel_tests, sequential_tests


def tail(filepath: str, buff_len: int = 1024) -> List[str]:
    with open(filepath, "rb") as f:
        # Get file size to avoid seeking before start of file
        f.seek(0, os.SEEK_END)
        file_size = f.tell()

        if file_size <= buff_len:
            # File is smaller than buffer, read from beginning
            f.seek(0)
        else:
            # File is larger, seek from end
            f.seek(-buff_len, os.SEEK_END)
            f.readline()  # Skip partial line

        data = f.read()
        return data.decode(errors="replace")


def run_pytest_and_collect_results(
    command: str, env: str, report_name: str, timeout: int = None
) -> Tuple[Result, bool, bool]:
    """
    Runs a pytest command and reports whether the run was cut short by a timeout.

    Returns `(test_result, timed_out, hard_killed)`. `timed_out` is True when the run did
    not finish on its own but was stopped by either:
      - the graceful xdist `--session-timeout` (pytest interrupts itself and writes the
        `xdist.dsession.Interrupted: session-timeout:` marker to the log), or
      - the hard subprocess backstop (`Shell.run` `SIGTERM`s/`SIGKILL`s pytest after
        `timeout` seconds, e.g. when a test hangs past the session-timeout).

    `hard_killed` is True for the second case only. The two differ in how much of the job
    is left: the graceful timeout is an ordinary budgeted stop with the rest of the job
    still available, while the backstop only fires after the session-timeout was already
    missed, which is what a hung run looks like and where the runner cancels the job while
    post-processing is still going. A caller shortening its work must key on this and not
    on `timed_out`.

    Callers use `timed_out` to tell an empty result set caused by expected time-budget
    exhaustion (best effort, may be downgraded to `SKIPPED`) apart from one caused by a
    real pytest/harness failure (no timeout - must stay `ERROR`).
    """

    run_sw = Utils.Stopwatch()

    test_result = Result.from_pytest_run(
        command=command,
        env=env,
        cwd="./tests/integration/",
        pytest_report_file=f"{temp_path}/pytest_{report_name}.jsonl",
        pytest_logfile=f"{temp_path}/pytest_{report_name}.log",
        logfile=f"{temp_path}/{report_name}.log",
        timeout=timeout,
    )

    timed_out = False
    hard_killed = False
    if "!!!!!!! xdist.dsession.Interrupted: session-timeout:" in tail(
        f"{temp_path}/{report_name}.log"
    ):
        timed_out = True
        test_result.info = "ERROR: session-timeout occurred during test execution"
        assert test_result.status == Result.Status.ERROR
        test_result.results.append(
            Result(
                name="Timeout",
                status=Result.Status.FAIL,
                info=test_result.info,
            )
        )
    elif timeout is not None and run_sw.duration >= timeout:
        # The graceful session-timeout marker is absent but the run still reached the
        # hard subprocess `timeout`: `Shell.run` killed pytest before it could finish
        # (a normal run returns well under `timeout`). Treat this as a timeout so an
        # empty result is reported as best-effort rather than as a harness failure.
        timed_out = True
        hard_killed = True

    return test_result, timed_out, hard_killed


def is_empty_best_effort_skip(
    is_flaky_check: bool,
    is_targeted_check: bool,
    has_results: bool,
    timed_out: bool,
) -> bool:
    """
    Decide whether a flaky/targeted check that produced no test results should be
    reported as `SKIPPED` (best effort) instead of falling through to `create_from`'s
    default `ERROR`.

    Only the expected timeout path is downgraded: a flaky/targeted run whose time budget
    was exhausted (graceful xdist session-timeout or the hard subprocess backstop) before
    any test produced a result is best-effort `SKIPPED`. When no timeout was observed, an
    empty result means pytest failed to produce any output for some other reason (it
    crashed before writing the jsonl report, or exited with a plugin/internal error and no
    test rows). That is a real harness failure and must stay `ERROR`, so this returns
    False.
    """
    return (is_flaky_check or is_targeted_check) and not has_results and timed_out


def finalize_llvm_coverage_status(R: Result, has_error: bool) -> bool:
    """Apply the coverage job's status rules to `R` and return the surviving `has_error`.

    A coverage shard must not block the pipeline on test failures, hence the `set_success`
    below. But a resource kill is not a test verdict: clearing `has_error` here would drop
    the leaf-OOM `ERROR` and let a run the cap killed finish green. `force_ok_exit` is set
    for coverage jobs regardless, so keeping the error only stops the job from *reporting*
    success.
    """
    has_failure = False
    for r in R.results:
        if r.status == Result.Status.FAIL:
            if r.has_label(Result.Label.OK_ON_RETRY):
                # Remove label and set to OK
                r.remove_label(Result.Label.OK_ON_RETRY)
                r.status = Result.Status.OK
            else:
                has_failure = True
    if has_failure:
        R.set_failed()
        R.set_info("Some tests failed during LLVM coverage run")
    elif has_error:
        pass  # the caller's `if has_error: set_error()` supplies the status and info
    else:
        R.set_success()
    return has_error


def main():
    sw = Utils.Stopwatch()
    info = Info()
    args = parse_args()
    job_params = args.options.split(",") if args.options else []
    job_params = [to.strip() for to in job_params]
    use_old_analyzer = False
    use_distributed_plan = False
    use_database_disk = False
    is_flaky_check = False
    is_bugfix_validation = False
    is_parallel = False
    is_sequential = False
    is_targeted_check = False
    is_llvm_coverage = False
    llvm_profdata_cmd = None

    # Set on_error_hook to collect logs on hard timeout
    Result.from_fs(info.job_name).set_on_error_hook(
        """
dmesg -T >./ci/tmp/dmesg.log
sudo chown -R $(id -u):$(id -g) ./tests/integration
tar -czf ./ci/tmp/logs.tar.gz \
  ./tests/integration/test_*/_instances*/ \
  ./ci/tmp/*.log \
  ./ci/tmp/*.jsonl || :
"""
    ).set_files(
        [
            "./ci/tmp/logs.tar.gz",
            "./ci/tmp/dmesg.log",
            DMESG_FOLLOW_LOG,
            DOCKER_IN_DOCKER_LOG,
        ],
        strict=False,
    )

    if args.param:
        for item in args.param.split(","):
            print(f"Setting env variable: {item}")
            key, _, value = item.partition("=")
            key = key.strip()
            if not key:
                continue
            os.environ[key] = value.strip()

    java_path = Shell.get_output(
        r"update-alternatives --config java | sed -n 's/.*(providing \/usr\/bin\/java): //p'",
        verbose=True,
    )
    repeat_option = ""
    if "bugfix" in info.job_name.lower():
        is_bugfix_validation = True

    batch_num, total_batches = 1, 1
    for to in job_params:
        if "/" in to:
            batch_num, total_batches = map(int, to.split("/"))
        elif any(build in to for build in ("amd_", "arm_")):
            if "amd_llvm_coverage" in to:
                is_llvm_coverage = True
        elif to == "old analyzer":
            use_old_analyzer = True
        elif to == "distributed plan":
            use_distributed_plan = True
        elif to == "db disk":
            use_database_disk = True
        elif to == "flaky":
            is_flaky_check = True
        elif to == "parallel":
            is_parallel = True
        elif to == "sequential":
            is_sequential = True
        elif "bugfix" in to.lower() or "validation" in to.lower():
            is_bugfix_validation = True
        elif "targeted" in to:
            is_targeted_check = True
        else:
            assert False, f"Unknown job option [{to}]"

    if args.count:
        repeat_option = f"--count {args.count} --random-order"
    # For flaky/targeted checks, --count is not used. Instead, --dist=each runs N workers
    # each executing all modules independently with their own isolated Docker cluster
    # (ClickHouseCluster appends PYTEST_XDIST_WORKER to project_name for isolation).

    # Read the budget here, not at import: `--param` above writes the environment it comes from.
    workers = planned_workers(
        args.workers,
        nested_budget_gb(),
        ncpu,
        dist_each=is_flaky_check or is_targeted_check,
    )

    clickhouse_path = f"{Utils.cwd()}/ci/tmp/clickhouse"
    clickhouse_server_config_dir = f"{Utils.cwd()}/programs/server"
    if info.is_local_run:
        if args.path:
            clickhouse_path = args.path
        else:
            paths_to_check = [
                clickhouse_path,  # it's set for CI runs, but we need to check it
                f"{Utils.cwd()}/build/programs/clickhouse",
                f"{Utils.cwd()}/clickhouse",
            ]
            for path in paths_to_check:
                if Path(path).is_file():
                    clickhouse_path = path
                    break
            else:
                raise FileNotFoundError(
                    "ClickHouse binary not found in any of the paths: "
                    + ", ".join(paths_to_check)
                    + ". You can also specify path to binary via --path argument"
                )
        if args.path_1:
            clickhouse_server_config_dir = args.path_1
    assert Path(
        clickhouse_server_config_dir
    ), f"ClickHouse config dir does not exist [{clickhouse_server_config_dir}]"
    print(f"Using ClickHouse binary at [{clickhouse_path}]")

    changed_test_modules = []
    if is_bugfix_validation or is_flaky_check or is_targeted_check:
        if info.is_local_run:
            assert (
                args.test
            ), "--test must be provided for flaky or bugfix job flavor with local run"
        else:
            if is_bugfix_validation and Labels.PR_BUGFIX not in info.pr_labels and Labels.PR_CRITICAL_BUGFIX not in info.pr_labels:
                # Not a bugfix PR - run a simple sanity test
                changed_test_modules = ["test_accept_invalid_certificate/test.py"]
            else:
                # TODO: reduce scope to modified test cases instead of entire modules
                changed_files = info.get_changed_files()
                for file in changed_files:
                    if Targeting.is_integration_test_file(file):
                        changed_test_modules.append(
                            file.removeprefix("tests/integration/")
                        )
                if not changed_test_modules and Labels.CI_FORCE_ALL in info.pr_labels:
                    print(
                        f"NOTE: No changed test modules found, but '{Labels.CI_FORCE_ALL}' label forces run - using sanity test"
                    )
                    changed_test_modules = ["test_accept_invalid_certificate/test.py"]

    # Best-effort scope cap for the flaky check (see MAX_FLAKY_CHECK_MODULES). When a PR
    # touches more changed test modules than can be repeatedly run within the time budget,
    # run a deterministic subset (sorted for reproducibility) and skip the rest rather than
    # truncating the whole run. The remaining ones are reported below as skipped so the
    # reduced coverage is explicit, not silent.
    skipped_flaky_modules = []
    if is_flaky_check and len(changed_test_modules) > MAX_FLAKY_CHECK_MODULES:
        changed_test_modules = sorted(changed_test_modules)
        skipped_flaky_modules = changed_test_modules[MAX_FLAKY_CHECK_MODULES:]
        changed_test_modules = changed_test_modules[:MAX_FLAKY_CHECK_MODULES]
        print(
            f"Flaky check: best-effort scope cap - running {len(changed_test_modules)} of "
            f"{len(changed_test_modules) + len(skipped_flaky_modules)} changed modules "
            f"to fit the time budget.\n"
            f"  Running: {changed_test_modules}\n"
            f"  Skipped (best effort): {skipped_flaky_modules}"
        )

    if is_bugfix_validation:
        # Download the master-HEAD binaries matching this job's runner arch:
        # the aarch64 job runs on an ARM runner and must use the ARM builds.
        build_types = bugfix_build_types(info.job_name)
        bt_paths = {bt: f"{temp_path}/clickhouse_{bt}" for bt in build_types}
        # In local runs, only reuse existing binaries; probing master commits in S3
        # depends on `master_commits` workflow data populated by CI workflow hooks
        # and is not available locally.
        if info.is_local_run:
            missing = [str(p) for p in bt_paths.values() if not Path(p).is_file()]
            assert not missing, (
                "Local bugfix validation requires all build-type binaries to be "
                f"present under {temp_path}; missing: {missing}"
            )
            build_urls = None
        else:
            build_urls = find_master_builds(build_types)
            assert build_urls, "Could not find master builds in S3"
        if build_urls:
            for bt, url in build_urls.items():
                bt_path = bt_paths[bt]
                if not info.is_local_run or not Path(bt_path).is_file():
                    print(f"NOTE: Downloading {bt} build to [{bt_path}]")
                    Shell.run(
                        f"wget -nv -O {bt_path} {url}", verbose=True, strict=True
                    )
                    Shell.run(f"chmod +x {bt_path}", verbose=True)
        clickhouse_path = f"{temp_path}/clickhouse_{build_types[0]}"

    if is_bugfix_validation or is_flaky_check:
        assert (
            changed_test_modules or (info.is_local_run and args.test)
        ), "No changed test modules found, either job must be skipped or bug in changed test search logic"

    Shell.check(f"chmod +x {clickhouse_path}", verbose=True, strict=True)
    Shell.check(f"{clickhouse_path} --version", verbose=True, strict=True)

    targeted_tests = []
    if is_targeted_check:
        assert not args.test, "--test not supposed to be used for targeted check ???"
        targeter = Targeting(info=info)
        tests, results_with_info = targeter.get_all_relevant_tests_with_info()
        # no subtask level for integration tests - cannot add this info to the report now
        # results.append(results_with_info)
        if not tests:
            # early exit
            Result.create_from(
                status=Result.Status.SKIPPED,
                info="No failed tests found from previous runs",
            ).complete_job()

        # Parse test names from the query result
        for test_ in tests:
            if test_.strip():
                test_name = test_.strip()
                targeted_tests.append(
                    test_name.split("[")[0]
                )  # remove parametrization - does not work with test repeat with --count
        print(f"Parsed {len(targeted_tests)} test names: {targeted_tests}")

    if not Shell.check("docker info > /dev/null 2>&1", verbose=True):
        start_docker_in_docker()
    Shell.check("docker info > /dev/null", verbose=True, strict=True)

    parallel_test_modules, sequential_test_modules = (
        get_parallel_sequential_tests_to_run(
            batch_num,
            total_batches,
            args.test or targeted_tests or changed_test_modules,
            workers,
            args.options,
            info,
            no_strict=is_targeted_check or is_flaky_check,  # targeted check might want to run test that was removed on a merge-commit; flaky check might pick up a changed test filtered out by SKIP_LIST in the private fork
        )
    )

    if is_flaky_check or is_targeted_check:
        # The flaky/targeted parallel bucket runs `--dist=each`: every worker runs
        # every parallel module at once. TEST_CONFIGS `dist_each_sequential` modules
        # would start one cluster per worker and OOM small runners, so move them to
        # the looped sequential phase. Normal `--dist=loadfile` runs do not call this.
        before = list(parallel_test_modules)
        parallel_test_modules, sequential_test_modules = force_heavy_modules_sequential(
            parallel_test_modules, sequential_test_modules
        )
        moved = [m for m in before if m not in parallel_test_modules]
        if moved:
            print(f"Forced heavy modules to the sequential phase (avoid concurrent --dist=each clusters): {moved}")

    if is_sequential:
        parallel_test_modules = []
        assert not is_parallel
    elif is_parallel:
        sequential_test_modules = []
        assert not is_sequential

    # If this PR only touches test files (no production/config code changed),
    # this batch only needs to run whichever of parallel_test_modules /
    # sequential_test_modules actually contains a changed module - the other
    # side would produce results identical to master and can be dropped
    # outright (saving the time to run it), and if neither side contains a
    # changed module the whole batch can be skipped. Placed after the
    # is_sequential/is_parallel handling above so it sees the modules this
    # job invocation will actually run, not the pre-flavor-filter set.
    if (
        total_batches > 1
        and not is_flaky_check
        and not is_targeted_check
        and not is_bugfix_validation
        and not is_llvm_coverage
        and not args.test
    ):
        changed_files = info.get_changed_files()
        if changed_files and all(
            Targeting.is_functional_test_file(f)
            or Targeting.is_integration_test_file(f)
            or Targeting.is_ci_job_script(f)
            for f in changed_files
        ):
            changed_integration_modules = {
                f.removeprefix("tests/integration/")
                for f in changed_files
                if Targeting.is_integration_test_file(f)
            }
            if not changed_integration_modules:
                Result.create_from(
                    status=Result.Status.SKIPPED,
                    info="Only non-integration test files changed in this PR - nothing for this job to run",
                ).complete_job()
            if not (changed_integration_modules & set(parallel_test_modules)):
                parallel_test_modules = []
            if not (changed_integration_modules & set(sequential_test_modules)):
                sequential_test_modules = []
            if not parallel_test_modules and not sequential_test_modules:
                Result.create_from(
                    status=Result.Status.SKIPPED,
                    info="Only test files changed in this PR and none of the changed test modules fall into this batch",
                ).complete_job()

    if (is_targeted_check or is_flaky_check) and not parallel_test_modules and not sequential_test_modules:
        # Targeted check: all selected tests were stale (removed or renamed since the CIDB record).
        # Flaky check: all changed tests were filtered out (e.g. by SKIP_LIST in the private fork).
        # Either way, skip gracefully instead of producing a "no results" error.
        skip_info = (
            "All targeted tests are stale (removed or renamed)"
            if is_targeted_check
            else "All changed tests were filtered out (e.g. by SKIP_LIST)"
        )
        Result.create_from(
            status=Result.Status.SKIPPED,
            info=skip_info,
        ).complete_job()

    if is_flaky_check or is_targeted_check:
        # Sort by module file so all tests from the same file are consecutive.
        # With --dist=each, pytest preserves CLI argument order and uses it as the
        # collection order. If tests from different modules interleave (e.g. CIDB
        # returns them sorted by failure time), pytest finalizes and re-enters
        # module-scoped fixtures between them, breaking tests that call
        # cluster.add_instance() inside the fixture.
        # For regular jobs, preserve the duration-aware ordering from get_optimal_test_batch.
        parallel_test_modules = sorted(parallel_test_modules, key=lambda t: t.split("::")[0])
        sequential_test_modules = sorted(sequential_test_modules, key=lambda t: t.split("::")[0])

    # Setup environment variables for tests
    for image_name, env_name in IMAGES_ENV.items():
        tag = info.docker_tag(image_name)
        if tag:
            print(f"Setting environment variable [{env_name}] to [{tag}]")
            os.environ[env_name] = tag
        else:
            assert False, f"No tag found for image [{image_name}]"

    # Pre-fetch all Docker images needed by the selected test suites.
    # This is done after IMAGES_ENV vars are set so tag resolution works correctly.
    # Fail fast here rather than discovering missing images mid-test-run.
    all_test_modules = parallel_test_modules + sequential_test_modules
    compose_files = get_compose_files_for_test_modules(all_test_modules)
    print(
        f"Compose files detected for this batch ({len(compose_files)}): "
        + ", ".join(str(f.name) for f in compose_files)
    )
    images_to_prefetch = get_images_from_compose_files(compose_files)
    if not prefetch_images(images_to_prefetch):
        prefetch_failure_result().complete_job()

    test_env = {
        "CLICKHOUSE_TESTS_BASE_CONFIG_DIR": clickhouse_server_config_dir,
        "CLICKHOUSE_TESTS_SERVER_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_BINARY": clickhouse_path,  # some test cases support alternative binary location
        "CLICKHOUSE_TESTS_CLIENT_BIN_PATH": clickhouse_path,
        "CLICKHOUSE_USE_OLD_ANALYZER": "1" if use_old_analyzer else "0",
        "CLICKHOUSE_USE_DISTRIBUTED_PLAN": "1" if use_distributed_plan else "0",
        "CLICKHOUSE_USE_DATABASE_DISK": "1" if use_database_disk else "0",
        "PYTEST_CLEANUP_CONTAINERS": "1",
        "JAVA_PATH": java_path,
        # PromQL compliance: deterministic JSON for upload hook (see promql_compliance_upload_hook.py).
        "COMPLIANCE_RESULT_FILE": os.environ.get(
            "COMPLIANCE_RESULT_FILE", os.path.join(temp_path, "promql_compliance_result.json")
        ),
    }
    if is_llvm_coverage:
        # %c enables continuous mode: the counters are memory-mapped into the
        # file and updated as the code runs, so the file is structurally valid
        # at every instant. Without it the profile is written only at process
        # exit, and a SIGKILL inside that multi-second write left a half-written
        # file whose header claims a bogus size - llvm-profdata then aborts with
        # "LLVM ERROR: out of memory" trying to honour it (observed repeatedly
        # on this shard family; see LLVM issue #50970). Requires the coverage
        # build to compile with -mllvm -runtime-counter-relocation.
        test_env["LLVM_PROFILE_FILE"] = "it-%c%4m.profraw"
        print(
            f"NOTE: This is LLVM coverage run, setting LLVM_PROFILE_FILE to [{test_env['LLVM_PROFILE_FILE']}]"
        )
        # Auto-detect available LLVM profdata tool
        for ver in ["22", "21", "20", "18", "19", "17", "16", ""]:
            cmd = f"llvm-profdata{'-' + ver if ver else ''}"
            if Shell.check(f"command -v {cmd}", verbose=False):
                llvm_profdata_cmd = cmd
                break

        if not llvm_profdata_cmd:
            print("ERROR: llvm-profdata not found in PATH")
        else:
            print(f"Using {llvm_profdata_cmd} to merge coverage files")

    test_results = []
    failed_tests_files = []

    has_error = False
    # Set when a pytest run was cut short by a timeout (graceful session-timeout or the
    # hard subprocess backstop). Used below to keep an empty flaky/targeted result a
    # best-effort SKIPPED only when a timeout actually exhausted the budget.
    timed_out = False
    # Only the hard backstop, which is where the job is about to be cancelled. See
    # `run_pytest_and_collect_results`.
    hard_killed = False
    session_timeout_parallel = 3600 * 2
    session_timeout_sequential = 3600

    if is_llvm_coverage:
        session_timeout_parallel = 7200
        session_timeout_sequential = 7200

    if args.session_timeout:
        session_timeout_parallel = args.session_timeout * 2
        session_timeout_sequential = args.session_timeout

    # Flaky-check soft timeout. Mirrors the pattern in `ci/jobs/functional_tests.py`:
    # bound the total time spent inside pytest so the job has headroom for cleanup,
    # log collection and reporting before the job is cancelled. Without this, a
    # flaky-check run over many modified test modules can be hard-killed, producing no
    # report at all instead of a best-effort partial one.
    #
    # The budget must stay well below the external ceiling at which a lone integration
    # job is cancelled (observed at ~80-90 min from job start in CI). The previous 90 min
    # was above that ceiling: the graceful xdist `--session-timeout` and the subprocess
    # hard-kill backstop never fired before the external cancellation, so the whole
    # process was killed and 0 results were reported (the job showed a red failure rather
    # than a best-effort report). 45 min matches the functional flaky check and leaves
    # ample room for the hard-kill backstop (+600s below), cleanup and reporting.
    FLAKY_CHECK_TIME_LIMIT = 45 * 60  # 45 min - kept below the external job-cancellation ceiling
    if is_flaky_check:
        elapsed_for_flaky = int(sw.duration)
        flaky_check_remaining_s = max(FLAKY_CHECK_TIME_LIMIT - elapsed_for_flaky, 60)
        print(
            f"Flaky-check time limit: {FLAKY_CHECK_TIME_LIMIT}s "
            f"(elapsed so far: {elapsed_for_flaky}s, remaining: {flaky_check_remaining_s}s)"
        )
        # Cap per-phase session timeouts so a single phase cannot consume the entire budget.
        session_timeout_parallel = min(session_timeout_parallel, flaky_check_remaining_s)
        session_timeout_sequential = min(session_timeout_sequential, flaky_check_remaining_s)

    error_info = []

    failed_test_cases = []

    # Clear dmesg to avoid false OOM detection from previous CI jobs on the same host.
    # Do this only in CI (non-local runs) and via a non-interactive privileged helper.
    # Every dmesg-derived verdict, leaf or host-wide, is only admissible on a buffer this cleared.
    dmesg_cleared = False
    # Follows the buffer this clears, because the tests below wrap it long before it is read.
    dmesg_follow_proc = None
    if not info.is_local_run:
        # `ci/tmp` is git-ignored, so the clean between jobs on a runner leaves this file behind
        # and a previous job's kernel record would be read and uploaded as this run's. Before the
        # clear, because the writer below is only started when the clear succeeds.
        Path(DMESG_FOLLOW_LOG).unlink(missing_ok=True)
        try:
            dmesg_cleared = Utils.clear_dmesg()
        except Exception as ex:
            print(f"Failed to clear dmesg before integration tests: {ex}")
        if not dmesg_cleared:
            print(
                "WARNING: could not clear dmesg before the tests; the buffer may still hold a "
                "previous job's records, so leaf OOMs will be reported from the counters only "
                "and a host OOM is not reportable at all on this run"
            )
        else:
            dmesg_follow_proc = start_dmesg_follow()

    clear_rabbitmq_recreation_scan_inputs()

    if is_flaky_check or is_targeted_check:
        # Each xdist worker runs all modules independently with its own isolated Docker cluster.
        # ClickHouseCluster appends PYTEST_XDIST_WORKER to the project name, so clusters
        # from different workers never interfere. --dist=each sends all tests to every worker.
        parallel_dist = "--dist=each"
        parallel_workers = workers
        # Sequential tests cannot run in parallel, so we loop over them instead.
        # Run at least 3 times to have meaningful flakiness signal, at most workers times.
        sequential_repeat_cnt = max(3, workers)
    else:
        parallel_dist = "--dist=loadfile"
        parallel_workers = workers
        sequential_repeat_cnt = 1

    if parallel_test_modules:
        log_file = f"{temp_path}/pytest_parallel.log"
        (
            test_result_parallel,
            parallel_timed_out,
            parallel_hard_killed,
        ) = run_pytest_and_collect_results(
            command=f"{quote_tests(parallel_test_modules)} --report-log-exclude-logs-on-passed-tests -n {parallel_workers} {parallel_dist} --tb=short {repeat_option} --session-timeout={session_timeout_parallel}",
            env=test_env,
            report_name="parallel",
            timeout=session_timeout_parallel + 600,
        )
        timed_out = timed_out or parallel_timed_out
        hard_killed = hard_killed or parallel_hard_killed
        test_results.extend(test_result_parallel.results)
        _mark_infrastructure_errors(test_result_parallel.results)
        failed_test_cases.extend(
            [t.name for t in test_result_parallel.results if t.is_failure()]
        )
        if test_result_parallel.files:
            failed_tests_files.extend(test_result_parallel.files)
        if test_result_parallel.is_error():
            if not is_targeted_check and not is_flaky_check:
                # In targeted checks we may overload the run with many heavy tests running
                # in parallel; in flaky checks the soft FLAKY_CHECK_TIME_LIMIT may cap the
                # pytest session-timeout. In both cases a session-timeout is an expected
                # risk rather than an infrastructure problem, so we do not treat such
                # errors as job-level failures.
                has_error = True
                error_info.append(test_result_parallel.info)

    fail_num = len([r for r in test_results if not r.is_ok()])
    # Under LLVM coverage the sequential phase runs regardless of how many
    # parallel tests failed: the coverage shard exists to execute code, and
    # dropping the phase would silently publish a profile that is short by
    # every sequential test. Test failures gate the PR through the regular
    # (non-coverage) jobs. An infrastructure error still skips the phase.
    if sequential_test_modules and (fail_num < MAX_FAILS_BEFORE_DROP or is_llvm_coverage) and not has_error:
        for attempt in range(sequential_repeat_cnt):
            # Recompute remaining budget for flaky-check at every iteration and stop
            # scheduling new runs once it is exhausted (soft timeout).
            iter_session_timeout_sequential = session_timeout_sequential
            if is_flaky_check:
                elapsed_for_flaky = int(sw.duration)
                flaky_check_remaining_s = max(FLAKY_CHECK_TIME_LIMIT - elapsed_for_flaky, 0)
                if flaky_check_remaining_s < 60:
                    print(
                        f"Flaky-check time limit reached after [{attempt}/{sequential_repeat_cnt}] sequential attempts "
                        f"(elapsed: {elapsed_for_flaky}s, limit: {FLAKY_CHECK_TIME_LIMIT}s); stopping"
                    )
                    break
                iter_session_timeout_sequential = min(
                    session_timeout_sequential, flaky_check_remaining_s
                )
            (
                test_result_sequential,
                sequential_timed_out,
                sequential_hard_killed,
            ) = run_pytest_and_collect_results(
                command=f"{quote_tests(sequential_test_modules)} --report-log-exclude-logs-on-passed-tests --tb=short {repeat_option} -n 1 --dist=loadfile --session-timeout={iter_session_timeout_sequential}",
                env=test_env,
                report_name="sequential",
                timeout=iter_session_timeout_sequential + 600,
            )
            timed_out = timed_out or sequential_timed_out
            hard_killed = hard_killed or sequential_hard_killed
            test_results.extend(test_result_sequential.results)
            _mark_infrastructure_errors(test_result_sequential.results)
            failed_test_cases.extend(
                [t.name for t in test_result_sequential.results if t.is_failure()]
            )
            if test_result_sequential.files:
                failed_tests_files.extend(test_result_sequential.files)
            if test_result_sequential.is_error():
                if not is_targeted_check and not is_flaky_check:
                    # In targeted checks we may overload the run with many heavy tests running
                    # sequentially; in flaky checks the per-iteration `iter_session_timeout_sequential`
                    # may be capped by FLAKY_CHECK_TIME_LIMIT. In both cases a session-timeout is
                    # an expected risk rather than an infrastructure problem, so we do not treat
                    # such errors as job-level failures.
                    has_error = True
                    error_info.append(test_result_sequential.info)
                break
            if (is_flaky_check or is_targeted_check) and not test_result_sequential.is_ok():
                print(
                    f"Flaky/targeted check: sequential test run fails after attempt [{attempt+1}/{sequential_repeat_cnt}] - break"
                )
                break

    # Run additional build types for bugfix validation.
    # Exit early on first failure to avoid duplicate test names and workspace pollution.
    if is_bugfix_validation:
        build_types = bugfix_build_types(info.job_name)
        for r in test_results:
            r.set_label(build_types[0])

        # `all` over an empty list is True, which is what a hard-killed primary run leaves, so
        # entry has to be gated too and not only the continuation below.
        if test_results and all(r.is_ok() for r in test_results) and not hard_killed:
            for bugfix_bt in build_types[1:]:
                print(f"\n=== Bugfix validation with {bugfix_bt} ===")
                bt_clickhouse_path = f"{temp_path}/clickhouse_{bugfix_bt}"
                test_env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = bt_clickhouse_path
                test_env["CLICKHOUSE_BINARY"] = bt_clickhouse_path
                test_env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = bt_clickhouse_path
                Shell.check(
                    f"{bt_clickhouse_path} --version", verbose=True, strict=True
                )

                bt_test_results = []

                if parallel_test_modules:
                    (
                        bt_result_parallel,
                        bt_parallel_timed_out,
                        bt_parallel_hard_killed,
                    ) = run_pytest_and_collect_results(
                        command=f"{quote_tests(parallel_test_modules)} --report-log-exclude-logs-on-passed-tests -n {workers} --dist=loadfile --tb=short {repeat_option} --session-timeout={session_timeout_parallel}",
                        env=test_env,
                        report_name=f"parallel_{bugfix_bt}",
                        timeout=session_timeout_parallel + 600,
                    )
                    timed_out = timed_out or bt_parallel_timed_out
                    hard_killed = hard_killed or bt_parallel_hard_killed
                    bt_test_results.extend(bt_result_parallel.results)
                    _mark_infrastructure_errors(bt_result_parallel.results)
                    if bt_result_parallel.files:
                        failed_tests_files.extend(bt_result_parallel.files)
                    if bt_result_parallel.is_error():
                        has_error = True
                        error_info.append(bt_result_parallel.info)

                bt_fail_num = len([r for r in bt_test_results if not r.is_ok()])
                if sequential_test_modules and bt_fail_num < MAX_FAILS_BEFORE_DROP and not has_error:
                    (
                        bt_result_sequential,
                        bt_seq_timed_out,
                        bt_seq_hard_killed,
                    ) = run_pytest_and_collect_results(
                        command=f"{quote_tests(sequential_test_modules)} --report-log-exclude-logs-on-passed-tests --tb=short {repeat_option} -n 1 --dist=loadfile --session-timeout={session_timeout_sequential}",
                        env=test_env,
                        report_name=f"sequential_{bugfix_bt}",
                        timeout=session_timeout_sequential + 600,
                    )
                    timed_out = timed_out or bt_seq_timed_out
                    hard_killed = hard_killed or bt_seq_hard_killed
                    bt_test_results.extend(bt_result_sequential.results)
                    _mark_infrastructure_errors(bt_result_sequential.results)
                    if bt_result_sequential.files:
                        failed_tests_files.extend(bt_result_sequential.files)
                    if bt_result_sequential.is_error():
                        has_error = True
                        error_info.append(bt_result_sequential.info)

                for r in bt_test_results:
                    r.set_label(bugfix_bt)
                test_results = bt_test_results

                if any(not r.is_ok() for r in bt_test_results):
                    break
                # A hard-killed run usually produces no results at all, and `any` over an empty
                # list is False, so the loop would schedule the remaining build types after a
                # backstop had already fired and postpone the diagnostics past the next one.
                if hard_killed or not bt_test_results:
                    print(
                        f"Bugfix validation with {bugfix_bt} produced no usable outcome; "
                        "not scheduling the remaining build types"
                    )
                    break

    # Before the archive below, which on this path is what the cancellation cuts off.
    if hard_killed and not info.is_local_run:
        print_timeout_diagnostics(
            os.environ, follow_proc=dmesg_follow_proc, dmesg_cleared=dmesg_cleared
        )

    # Collect logs before re-run
    attached_files = []
    # Leaf-OOM rows the scan below reports, so the second scan after the `/init` work can tell a
    # new breach from one it is merely seeing again.
    reported_leaf_ooms: Set[str] = set()
    if not info.is_local_run:
        failed_suits = []
        # Collect docker compose configs used in tests
        config_files = [
            str(p)
            for p in Path("./tests/integration/").glob("test_*/_instances*/*/configs/")
        ]
        for test_result in test_results:
            if not test_result.is_ok() and ".py" in test_result.name:
                failed_suits.append(test_result.name.split("/")[0])
        failed_suits = list(set(failed_suits))
        for failed_suit in failed_suits:
            failed_tests_files.append(f"tests/integration/{failed_suit}")

        # Add all files matched ./ci/tmp/*.log ./ci/tmp/*.jsonl into failed_tests_files
        for pattern in ["*.log", "*.jsonl"]:
            for log_file in Path("./ci/tmp/").glob(pattern):
                if log_file.is_file():
                    failed_tests_files.append(str(log_file))

        # A contained OOM leaves no failing `*.py` row to key on: `_mark_infrastructure_errors`
        # relabels the killed container's `Connection reset by peer` to the successful `SKIPPED`.
        _, leaf_breached = leaf_oom_report(os.environ, b"")
        if failed_suits or leaf_breached:
            # Bounded and non-fatal only on the timed-out path: elsewhere the archive has the
            # time it needs, and failing to produce it is a real error.
            archive_deadline = (
                time.monotonic() + TIMED_OUT_ARCHIVE_TIMEOUT if hard_killed else None
            )
            for files, name in (
                (failed_tests_files, "logs.tar.gz"),
                (config_files, "configs.tar.gz"),
            ):
                # One deadline for all of them, so two bounds cannot sum past the window. A
                # deadline already spent leaves nothing to archive with, hence the floor of 1.
                remaining = (
                    max(int(archive_deadline - time.monotonic()), 1)
                    if archive_deadline
                    else None
                )
                archive = Utils.compress_files_gz(
                    files,
                    f"{temp_path}/{name}",
                    timeout=remaining,
                    strict=not hard_killed,
                )
                if archive:
                    attached_files.append(archive)
                else:
                    print(
                        f"WARNING: could not archive {name} within {remaining}s; the "
                        "diagnostics printed above are what remains"
                    )
            if Path(DOCKER_IN_DOCKER_LOG).exists():
                attached_files.append(DOCKER_IN_DOCKER_LOG)

    # Rerun failed tests if any to check if failure is reproducible
    if 0 < len(failed_test_cases) < 10 and not (
        is_flaky_check or is_bugfix_validation or is_targeted_check or info.is_local_run
    ):
        test_result_retries, _, _ = run_pytest_and_collect_results(
            command=f"{quote_tests(failed_test_cases)} --report-log-exclude-logs-on-passed-tests --tb=short -n 1 --dist=loadfile --session-timeout=1200",
            env=test_env,
            report_name="retries",
            timeout=1200 + 600,
        )
        successful_retries = [t.name for t in test_result_retries.results if t.is_ok()]
        failed_retries = [t.name for t in test_result_retries.results if t.is_failure()]
        if successful_retries or failed_retries:
            for test_case in test_results:
                if test_case.name in successful_retries:
                    test_case.set_label(Result.Label.OK_ON_RETRY)
                elif test_case.name in failed_retries:
                    test_case.set_label(Result.Label.FAILED_ON_RETRY)

    # Remove iptables rule added in tests
    Shell.check("sudo iptables -D DOCKER-USER 1 ||:", verbose=True)

    # The leaf counters read this container's own cgroup, so they work on every run; only the
    # host-wide dmesg scan needs a CI host. An empty dmesg leaves the counters as the sole source,
    # which is what a local run gets. Without it a contained kill is invisible there: the killed
    # container's client raises `Connection reset by peer`, `_mark_infrastructure_errors` relabels
    # that `SKIPPED`, and `SKIPPED` is a successful status.
    dmesg = b""
    # Whether this dump succeeded. Neither the buffer nor the path answers that: a successful
    # dump can legitimately be empty, and a failed one still leaves the redirect's empty file.
    dmesg_dumped = False
    # Whether the record spans the run. Both halves are needed: a follower still running proves
    # the earlier window, and only the snapshot proves the tail it has not consumed yet.
    dmesg_covers_run = False
    if not info.is_local_run:
        print("Dumping dmesg")
        follow_dmesg = read_dmesg_follow()
        # Polled where the record is read, so a follower alive here consumed the buffer up to it.
        follow_alive = dmesg_follow_proc is not None and dmesg_follow_proc.poll() is None
        # Not `strict`: raising here would skip the report this dump feeds, so a run whose dmesg
        # is unreadable would lose the counter-based reports too, which do not need dmesg at all.
        if Shell.check("dmesg -T > ./ci/tmp/dmesg.log", verbose=True):
            dmesg_dumped = True
            with open("./ci/tmp/dmesg.log", "rb") as dmesg_file:
                # A superset of the snapshot alone, so no detector below can lose a signal. The
                # overlap stays: only `print_oom_lines` renders per match, and deduping at `-T`'s
                # one-second resolution would merge distinct same-second kills.
                dmesg = follow_dmesg + dmesg_file.read()
        else:
            print(
                "WARNING: could not dump dmesg; leaf OOMs will be reported from the counters only"
            )
            dmesg = follow_dmesg
        dmesg_covers_run = follow_alive and dmesg_dumped

    # `ERROR` plus `has_error` matches the existing OOM treatment on every path, not just
    # bugfix validation - a resource kill is never a test verdict, and the `ERROR` keeps the
    # status inversion below from flipping the job green.
    leaf_scoped_dmesg = dmesg if dmesg_cleared else b""
    leaf_results, attach_dmesg = leaf_oom_report(os.environ, leaf_scoped_dmesg)
    for leaf_result in leaf_results:
        test_results.append(leaf_result)
        reported_leaf_ooms.add(leaf_result.name)
        has_error = True
        error_info.append(
            f"{leaf_result.name} - infrastructure/resource failure, "
            "not bug reproduction"
        )

    is_bugfix_validation_labelled = is_bugfix_validation and (
        Labels.PR_BUGFIX in info.pr_labels or Labels.PR_CRITICAL_BUGFIX in info.pr_labels
    )

    if not info.is_local_run:
        host_oom_in_dmesg = any(
            pattern in dmesg for pattern in HOST_OOM_DMESG_PATTERNS
        )
        if host_oom_in_dmesg:
            # Attached whichever run the kill belongs to: reading an unattributable dump is
            # useful, and only the verdict below needs a buffer holding this run alone.
            attach_dmesg = True
        if host_oom_in_dmesg and not dmesg_cleared:
            print(
                "WARNING: dmesg records a host OOM but the buffer was not cleared for this "
                "run, so it may be a previous job's; not reporting it as this job's failure"
            )
        if host_oom_in_dmesg and dmesg_cleared:
            if is_bugfix_validation:
                # A host OOM is an infrastructure/resource failure, not bug
                # reproduction. Report it as `ERROR` and set `has_error` so
                # the status inversion below is skipped (same as `Timeout`),
                # otherwise the inverted `FAIL` would flip the job to green.
                test_results.append(
                    Result(name=OOM_IN_DMESG_TEST_NAME, status=Result.Status.ERROR)
                )
                has_error = True
                error_info.append(
                    "OOM in dmesg - infrastructure/resource failure, "
                    "not bug reproduction"
                )
            else:
                test_results.append(
                    Result(name=OOM_IN_DMESG_TEST_NAME, status=Result.Status.FAIL)
                )

        # Every failure, not only the two the verdicts above can attribute: on cgroup v1 a kill
        # inside a test container is charged to that container's own cgroup, so no verdict here
        # sees one and the dump is its only record.
        if (
            attach_dmesg
            or has_error
            # The synthetic session-timeout row is not a failure on its own: it is stripped below
            # and the run can still report OK or best-effort SKIPPED. A real failure leaves its own.
            or any(not r.is_ok() for r in test_results if r.name != "Timeout")
            # Mirrors `empty_harness_failure` below, which reports ERROR without setting `has_error`.
            or ((is_flaky_check or is_targeted_check) and not test_results and not timed_out)
            # Mirrors `had_infra_or_error` below: it reports ERROR off the `INFRA` label alone,
            # which leaves no row non-OK and never sets `has_error`.
            or (
                is_bugfix_validation_labelled
                and any(r.has_label(Result.Label.INFRA) for r in test_results)
            )
        ):
            print_oom_lines(
                dmesg.decode(errors="replace"),
                caveat="" if dmesg_cleared else UNCLEARED_DMESG_CAVEAT,
                partial="" if dmesg_covers_run else PARTIAL_DMESG_CAVEAT,
            )
            if dmesg_dumped:
                attached_files.append("./ci/tmp/dmesg.log")
            if Path(DMESG_FOLLOW_LOG).exists():
                attached_files.append(DMESG_FOLLOW_LOG)

    # For targeted, flaky checks, and bugfix validation, the synthetic "Timeout"
    # result must not be propagated as a top-level `FAIL`: for targeted checks a
    # session-timeout is an expected risk (because of `--count N` overloading), for
    # flaky checks because of the soft `FLAKY_CHECK_TIME_LIMIT`, and for bugfix
    # validation an inverted `FAIL` would be mistakenly treated as successful bug
    # reproduction.
    if is_targeted_check or is_flaky_check or is_bugfix_validation:
        test_results = [r for r in test_results if r.name != "Timeout"]

    # Whether pytest produced any real test results, captured *before* the synthetic
    # `skipped_flaky_modules` entries are appended below. The empty-result status decision
    # must look only at real pytest output: in a scope-capped flaky run the synthetic
    # `SKIPPED` entries would otherwise make `test_results` non-empty even when the selected
    # modules produced nothing, masking a timeout-empty run (should be `SKIPPED`) or an empty
    # harness failure (should be `ERROR`) as a green top-level result.
    pytest_has_results = bool(test_results)

    # Make the best-effort scope cap explicit in the report: list the modules that were
    # skipped to fit the time budget as SKIPPED entries rather than dropping them silently.
    for skipped_module in skipped_flaky_modules:
        test_results.append(
            Result(
                name=skipped_module,
                status=Result.Status.SKIPPED,
                info="Skipped by flaky-check best-effort scope cap (MAX_FLAKY_CHECK_MODULES)",
            )
        )

    # If a timeout exhausted the time budget before any test produced a result (e.g. a
    # single very slow or hanging module consumed the whole budget), report SKIPPED rather
    # than letting `create_from` default an empty result set to ERROR, which would block
    # the PR. Crucially, this best-effort downgrade applies *only* when a timeout was
    # actually observed: an empty result without a timeout means pytest failed to produce
    # any output for some other reason (crashed before writing the jsonl report, plugin or
    # internal error, ...), which is a real harness failure and must stay ERROR.
    #
    # Both decisions use `pytest_has_results` (real pytest output) rather than the current
    # `test_results`, which may carry only the synthetic `skipped_flaky_modules` entries
    # appended above. With a scope cap in effect those synthetic `SKIPPED` rows make
    # `test_results` non-empty, so the empty-result status must be forced here: `create_from`
    # only defaults an empty result set to `ERROR`, and a list of `SKIPPED` rows would
    # otherwise collapse to a green top-level status.
    empty_best_effort = is_empty_best_effort_skip(
        is_flaky_check, is_targeted_check, pytest_has_results, timed_out
    )
    empty_harness_failure = (
        (is_flaky_check or is_targeted_check) and not pytest_has_results and not timed_out
    )
    R = Result.create_from(
        results=test_results,
        status=(
            Result.Status.SKIPPED
            if empty_best_effort
            else Result.Status.ERROR if empty_harness_failure else ""
        ),
        info=(
            "No test results collected within the flaky-check time budget (best effort)"
            if empty_best_effort
            else (
                "No test results collected and no timeout was observed - reporting the "
                "empty pytest run as a harness ERROR"
                if empty_harness_failure
                else ""
            )
        ),
        stopwatch=sw,
        files=attached_files,
    )

    # Snapshot the run's health before the blocks below launder `has_error`;
    # a timed-out or errored run must publish no coverage profile.
    coverage_run_complete = not timed_out and not has_error

    if is_llvm_coverage:
        assert (
            is_bugfix_validation is False
        ), "LLVM coverage with bugfix validation is not supported"
        has_error = finalize_llvm_coverage_status(R, has_error)

    # Capture whether this run saw any infrastructure problems BEFORE the
    # clearing block below resets `has_error`. If the answer is yes, the
    # bugfix-validation inversion path further down must be skipped: we have
    # no reliable signal about whether the bug reproduces on this arch, and
    # running the inversion would let an infra `FAIL` be flipped to `OK`
    # (counted as validation) or rewrite an `ERROR`-only outcome as
    # `SKIPPED` via the no-`has_failure` branch. See bot review on
    # ClickHouse/ClickHouse#103541 (2026-05-15).
    had_infra_or_error = has_error or any(
        r.has_label(Result.Label.INFRA) for r in test_results
    )

    # If all non-OK results are infrastructure errors, do not treat as a real failure
    if has_error:
        non_ok = [r for r in test_results if not r.is_ok()]
        if non_ok and all(r.has_label(Result.Label.INFRA) for r in non_ok):
            print(
                "All failures are infrastructure errors - clearing error flag"
            )
            has_error = False
            force_ok_exit = True

    if has_error:
        R.set_error().set_info("\n".join(error_info))

    if is_bugfix_validation_labelled:
        assert (
            is_llvm_coverage is False
        ), "Bugfix validation with LLVM coverage is not supported"
        if had_infra_or_error:
            # Infrastructure errors or session-level failures were observed
            # during this run. Skip the inversion path so the per-arch job
            # cannot be silently counted as a validation. The post-hook in
            # `new_tests_check.py` uses strict `is_success` (`OK` / `XFAIL`
            # only); leaving the result in a non-success state is enough to
            # prevent this arch from contributing a false validation.
            #
            # If, after all the upstream handling, the result is still in a
            # success-equivalent state (e.g. every surviving child is `OK`
            # because all infra failures were already relabeled to `SKIPPED`
            # by `_mark_infrastructure_errors`), force `ERROR` here so the
            # post-hook cannot accidentally treat this arch as validated.
            print(
                "Bugfix validation: infrastructure error or session-level "
                "failure detected - skipping status inversion to avoid "
                "leaking an infra outcome into validation success."
            )
            if R.is_success():
                R.set_error().set_info(
                    "Bugfix validation aborted: infrastructure error during "
                    "the run - no reliable signal about whether the bug "
                    "reproduces on this arch"
                )
        else:
            has_failure = False
            for r in R.results:
                # invert statuses: only `FAIL` is treated as a successful
                # reproduction signal. Generic `ERROR` is left untouched
                # because in integration tests `ERROR` is also used for
                # runner-level problems (for example session-timeout paths
                # in `run_pytest_and_collect_results`), and infrastructure
                # errors that escape `_mark_infrastructure_errors` could
                # otherwise flip the job to green.
                r.set_label(Result.Label.XFAIL)
                if r.status == Result.Status.FAIL:
                    r.status = Result.Status.OK
                    has_failure = True
                elif r.status == Result.Status.OK:
                    r.status = Result.Status.FAIL
            if not has_failure:
                # See the matching comment in `ci/jobs/functional_tests.py`. The
                # bug did not reproduce on this arch, so report SKIPPED instead
                # of FAIL: `Result.is_ok` includes SKIPPED so the job exits 0,
                # while `is_success` (used by the post-hook) excludes SKIPPED so
                # the per-arch job does not count as a validation. Contract:
                # at least one per-arch job must end up `OK`/`XFAIL` for the
                # post-hook to consider the bug validated.
                print("Bug does not reproduce on this arch - bugfix validation N/A")
                R.set_status(Result.Status.SKIPPED)
                R.set_info("Bug does not reproduce on this arch - bugfix validation N/A")
            else:
                R.set_success()

    force_ok_exit = False
    if R:
        failures_cnt = len([r for r in R.results if not r.is_ok()])
        if failures_cnt > 0 and failures_cnt < 2:
            print(
                f"NOTE: Failed {failures_cnt} tests - do not block pipeline, exit with 0"
            )
            force_ok_exit = True
        elif failures_cnt > 0 and "ci-non-blocking" in info.pr_labels:
            print(
                f"NOTE: Failed {failures_cnt} tests, label 'ci-non-blocking' is set - do not block pipeline - exit with 0"
            )
            force_ok_exit = True
    if is_bugfix_validation:
        # Per-arch bugfix-validation jobs are advisory: their pass/fail status
        # records "did the bug reproduce on this arch?", not whether the PR
        # should be blocked. Setting `do_not_block_pipeline_on_failure=True`
        # marks the job as non-blocking so downstream jobs are not dropped
        # when this job reports FAIL. The process itself still exits with
        # the natural status (`Result.complete_job` calls `sys.exit(1)` on
        # non-OK results); the non-blocking flag is metadata for the
        # pipeline scheduler. The PR-merge-blocking decision lives in the
        # `new_tests_check.py` workflow post-hook, which OR's the per-arch
        # bugfix-validation job statuses.
        print(
            "NOTE: Bugfix validation job - marking as non-blocking; "
            "failure here will not block downstream pipeline jobs "
            "(process exit code still reflects the actual job status)"
        )
        force_ok_exit = True
    lost_coverage_artifact = False
    if is_llvm_coverage and llvm_profdata_cmd:
        print("Collecting and merging LLVM coverage files...")

        # Merge all profraw files into final profdata file
        merged_profdata = merge_profraw_files(
            llvm_profdata_cmd, run_complete=coverage_run_complete
        )

        # Attach profdata file to the result report so it is uploaded
        # unconditionally (even when tests fail) and visible in the CI report.
        if merged_profdata and os.path.exists(merged_profdata):
            R.files.append(merged_profdata)
        else:
            lost_coverage_artifact = True

        force_ok_exit = True
        print("NOTE: LLVM coverage job - do not block pipeline - exit with 0")

    # After the last `/init` work, so the peaks cover the coverage merge too.
    print_leaf_peak_usage(os.environ)

    # The merge above is the last `/init` work and is not raised on failure, so a leaf the cap
    # killed there would reach `complete_job` unreported. Re-dump dmesg: a kill this late is only
    # in a fresh read, while the counters this also rereads are cumulative. As with the first scan,
    # the counters work on every run and only the dmesg dump needs a CI host.
    late_dmesg = b""
    # Whether this run has a dmesg it can attribute a leaf with, which an empty buffer does not
    # answer: a successful dump can legitimately be empty, and the file only exists on this path.
    late_dmesg_dumped = False
    # And whether that record spans the run, which is the stronger property the gap warning below
    # needs: a dump of a wrapped buffer succeeds while holding none of the window it is asked about.
    late_dmesg_covers_run = False
    if not info.is_local_run and dmesg_cleared:
        late_follow_dmesg = read_dmesg_follow()
        late_follow_alive = (
            dmesg_follow_proc is not None and dmesg_follow_proc.poll() is None
        )
        # Read only what this re-dump wrote: a surviving earlier file would report a kill
        # during the merge as absent.
        if Shell.check(f"dmesg -T > {LATE_DMESG_LOG}", verbose=True):
            late_dmesg_dumped = True
            with open(LATE_DMESG_LOG, "rb") as late_dmesg_file:
                late_dmesg = late_follow_dmesg + late_dmesg_file.read()
        else:
            print(
                "WARNING: could not re-dump dmesg after the coverage merge; a leaf killed there "
                "is only reportable from the counters"
            )
            late_dmesg = late_follow_dmesg
        late_dmesg_covers_run = late_follow_alive and late_dmesg_dumped
    for meaning in dind_unreportable_ooms(os.environ, late_dmesg_covers_run):
        print(
            f"WARNING: {meaning} cannot be detected on this run (cgroup v1 charges the kill to "
            "the victim's own cgroup, and no dmesg covering this run is available), so a green "
            "result does not rule it out"
        )
    late_breach = report_late_leaf_ooms(
        R,
        os.environ,
        late_dmesg,
        reported_leaf_ooms,
        lost_coverage_artifact=lost_coverage_artifact,
    )
    # Only on the path that produced the file.
    if late_breach and late_dmesg_dumped and LATE_DMESG_LOG not in R.files:
        R.files.append(LATE_DMESG_LOG)
    # A run whose only failure is a late breach never met the attach block above, so the record
    # that names the breach is otherwise not uploaded at all.
    if late_breach and Path(DMESG_FOLLOW_LOG).exists() and DMESG_FOLLOW_LOG not in R.files:
        R.files.append(DMESG_FOLLOW_LOG)

    # The last reader of the follow log is above, and every run that started a follower reaches
    # here: the only `complete_job` after the start is the one below, and nothing returns in
    # between. A run the runner hard-kills instead leaves it to die with the job's container.
    stop_dmesg_follow(dmesg_follow_proc)

    report_rabbitmq_recreations(R)

    R.sort().complete_job(do_not_block_pipeline_on_failure=force_ok_exit)


if __name__ == "__main__":
    main()
