from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

# Weekly jemalloc safety-check fuzzing.
#
# Builds `amd_debug` with -DENABLE_JEMALLOC_SAFETY_CHECKS=1 and runs the AST fuzzer
# against it. The option arms `config_opt_safety_checks` and `config_opt_size_checks`,
# which jemalloc ships but which are not defined in any ClickHouse build (Debug
# included), so a sized-deallocation mismatch or a double free is detected only much
# later, as an unattributable SIGSEGV in the background decay thread (issue #85726).
# Armed, `arena_ptr_array_flush_impl` (`arena.c`) and `maybe_check_alloc_ctx`
# (`jemalloc_internal_inlines_c.h`, which also catches a mismatched slab bit) abort on
# the *offending* thread with the true size, the claimed size and the pointer.
#
# Scope: the AST fuzzer only, on x86-64 only. The AST fuzzer is the one lane where both
# confirmed occurrences of that SIGSEGV appeared. x86-64 is a hard constraint of the
# option itself: `include_linux_e2k/.../jemalloc_internal_defs.h.in` carries bare
# `#undef JEMALLOC_OPT_SAFETY_CHECKS`, which would cancel the definition.
#
# This lane is diagnostic, not a gate. The sized-deallocation and double-free checks
# fire only on a size or state mismatch jemalloc's own invariants forbid, so a failure
# there is a real memory-safety bug. (`JEMALLOC_OPT_SAFETY_CHECKS` also arms the
# profiling redzone verification, `arena.c:597`/`:637`, which the AST fuzzer does not
# reach: sampling stays off unless a query sets `jemalloc_enable_profiler`.)
#
# Runs every Monday at 05:00 UTC.
workflow = Workflow.Config(
    name="WeeklyJemallocSafety",
    event=Workflow.Event.SCHEDULE,
    branches=[BASE_BRANCH],
    jobs=[
        *JobConfigs.jemalloc_safety_build_job,
        *JobConfigs.jemalloc_safety_ast_fuzzer_job,
    ],
    artifacts=[
        *ArtifactConfigs.clickhouse_binaries,
    ],
    dockers=DOCKERS,
    secrets=SECRETS,
    enable_cache=True,
    enable_report=True,
    enable_cidb=True,
    cron_schedules=["0 5 * * 1"],
)

WORKFLOWS = [
    workflow,
]
