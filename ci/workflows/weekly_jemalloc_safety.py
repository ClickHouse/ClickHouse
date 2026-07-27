from praktika import Workflow

from ci.defs.defs import BASE_BRANCH, DOCKERS, SECRETS, ArtifactConfigs
from ci.defs.job_configs import JobConfigs

# Weekly jemalloc safety-check fuzzing.
#
# Builds `amd_debug` with -DENABLE_JEMALLOC_SAFETY_CHECKS=1 and runs the AST fuzzer
# against it. The option arms `config_opt_safety_checks` and `config_opt_size_checks`,
# which jemalloc ships but which are `#undef` in every ClickHouse build (including
# Debug), so today a sized-deallocation mismatch or a double free silently mutates
# `edata_t` metadata and is noticed much later by the background decay thread as a
# SIGSEGV in `edata_list_inactive_remove` with no way to attribute it. With the checks
# on, `arena_ptr_array_flush_impl` and `pac_dalloc` abort on the *offending* thread with
# the true and claimed sizes and the offending pointer, which is what makes the report
# actionable.
#
# Scope: the AST fuzzer only, on x86-64 only. The AST fuzzer is the one lane where both
# confirmed occurrences of that SIGSEGV appeared. x86-64 is a hard constraint of the
# option itself: `include_linux_e2k/.../jemalloc_internal_defs.h.in` carries bare
# `#undef JEMALLOC_OPT_SAFETY_CHECKS`, which would cancel the definition.
#
# This lane is diagnostic, not a gate: it produces an attributable stack the next time
# the corruption happens. A failure here is a real memory-safety bug in ClickHouse (or a
# genuine jemalloc bug), never a false positive of the checks themselves - both checks
# only fire on a size or state mismatch that jemalloc's own invariants forbid.
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
