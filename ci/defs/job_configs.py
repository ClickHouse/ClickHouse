from praktika import Job
from praktika.utils import Utils

from ci.defs.defs import (
    LLVM_ARTIFACTS_LIST,
    LLVM_FT_NUM_BATCHES,
    LLVM_FT_OLD_S3_DB_REPL_NUM_BATCHES,
    LLVM_FT_OLD_S3_DB_REPL_SEQUENTIAL_NUM_BATCHES,
    LLVM_IT_NUM_BATCHES,
    ArtifactNames,
    BuildTypes,
    JobNames,
    RunnerLabels,
)
LIMITED_MEM = Utils.physical_memory() - 2 * 1024**3
# Keeper stress spins nested Docker inside the integration-tests-runner container.
# Using nearly all host RAM for the outer container can starve the host runner
# and lead to "runner lost communication". Reserve a larger margin on the host
# by capping Keeper to ~70% of physical memory.
KEEPER_DIND_MEM = Utils.physical_memory() * 70 // 100

# Integration tests run a nested Docker daemon, so `docker_in_docker.sh` splits the job's
# `--memory` into capped cgroup leaves. `/init`'s cap is a ceiling rather than a share, so the
# caps can sum above the job limit.

# Not a leaf: headroom for the pages faulted before delegation, which stay charged to the
# cgroup root and can be neither migrated nor reclaimed (measured up to ~147 MiB).
INTEGRATION_DIND_ROOT_RESERVE = 1 * 1024**3
# Everything not charged to `/docker`: pytest, its xdist workers, the post-teardown steps, and the
# per-test subprocesses - `helpers/cluster.py` runs each node's `clickhouse-client` and
# `helpers/iceberg_utils.py` runs Spark's `local` driver on the host, not in a container.
#
# A flat floor rather than the third of the job limit this used to take, because the share was
# claiming memory `/docker` cannot do without. Measured over 58 integration jobs on the 61.78 GiB
# runner: `/docker` peaks at its cap in every single one, while `/init` peaks at 20.8 GiB
# (median) against this reserve and `/dockerd` at 18.3 GiB against its 2 GiB one, and the job
# cgroup never breaches - the two overruns are page cache (the log archiving here, the image
# layers there) and reclaim gives them back. `/docker` is the opposite: read off a kernel OOM
# report, its 40.35 GiB charge is anonymous memory against 753664 bytes of cache, so not one page
# of it is reclaimable and its cap is the only one that binds. Sizing the unreclaimable leaf last
# is what put `Container memory budget exceeded (/docker)` on nearly every run of the ASan+UBSan
# shards.
INTEGRATION_DIND_INIT_RESERVE = 8 * 1024**3
# Holds dockerd, containerd and one containerd-shim per nested container, so it scales with
# concurrency rather than staying at the daemon's own footprint. An absolute floor, never a
# fraction of the job limit: too small and the daemons cannot boot at all.
INTEGRATION_DIND_DAEMON_RESERVE = 2 * 1024**3
# What the nested test containers may collectively use. Also bounds xdist worker concurrency, so
# scheduling and containment agree on one number. Clamped at zero because a negative reads to
# `docker_in_docker.sh`'s validator as a malformed variable rather than a host too small.
INTEGRATION_NESTED_BUDGET = max(
    LIMITED_MEM
    - INTEGRATION_DIND_ROOT_RESERVE
    - INTEGRATION_DIND_INIT_RESERVE
    - INTEGRATION_DIND_DAEMON_RESERVE,
    0,
)
# `/init`'s ceiling, not its share: its peak is one test's host-side client fan-out plus the page
# cache of the logs it reads and archives, and neither is bounded by the reserve above. It
# overlaps `/docker`, so this cap alone is within the job limit but the three together are not -
# which is what lets the reserve shrink without `/init` losing any room it actually uses.
INTEGRATION_DIND_INIT_LIMIT = max(
    LIMITED_MEM - INTEGRATION_DIND_ROOT_RESERVE - INTEGRATION_DIND_DAEMON_RESERVE,
    INTEGRATION_DIND_INIT_RESERVE,
)
# `/dockerd`'s ceiling, not its share, for the same reason `/init` has one: an image pull writes
# every layer through this leaf, so the leaf also holds that page cache, and a dirty page cannot be
# reclaimed until its writeback completes. The reserve stays the daemons' own anon footprint, which
# is what the other leaves must leave room for. Overlaps `/docker` exactly as `/init` does.
INTEGRATION_DIND_DAEMON_LIMIT = max(
    LIMITED_MEM - INTEGRATION_DIND_ROOT_RESERVE - INTEGRATION_DIND_INIT_RESERVE,
    INTEGRATION_DIND_DAEMON_RESERVE,
)
integration_dind_env = (
    "+--env=CI_DIND_REQUIRE_CGROUP_CONTAINMENT=1"
    f"+--env=CI_DIND_JOB_MEM={LIMITED_MEM}"
    f"+--env=CI_DIND_ROOT_RESERVE={INTEGRATION_DIND_ROOT_RESERVE}"
    f"+--env=CI_DIND_INIT_RESERVE={INTEGRATION_DIND_INIT_RESERVE}"
    f"+--env=CI_DIND_INIT_LIMIT={INTEGRATION_DIND_INIT_LIMIT}"
    f"+--env=CI_DIND_DAEMON_RESERVE={INTEGRATION_DIND_DAEMON_RESERVE}"
    f"+--env=CI_DIND_DAEMON_LIMIT={INTEGRATION_DIND_DAEMON_LIMIT}"
    f"+--env=CI_DIND_NESTED_BUDGET={INTEGRATION_NESTED_BUDGET}"
)

BINARY_DOCKER_COMMAND = (
    "clickhouse/binary-builder+--network=host"
    f"+--memory={Utils.physical_memory() * 95 // 100}"
    f"+--memory-reservation={Utils.physical_memory() * 9 // 10}"
    f"+--volume=.:/ClickHouse"
)

if Utils.is_arm():
    docker_sock_mount = "--volume=/var/run:/run/host:ro"
else:
    docker_sock_mount = "--volume=/run:/run/host:ro"

build_digest_config = Job.CacheDigestConfig(
    include_paths=[
        "./src",
        "./contrib/",
        "./.gitmodules",
        "./CMakeLists.txt",
        "./PreLoad.cmake",
        "./cmake",
        "./base",
        "./programs",
        "./rust",
        "./ci/jobs/build_clickhouse.py",
        "./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        "./ci/jobs/scripts/log_cluster.py",
        "./utils/prepare-time-trace/prepare-time-trace.sh",
        # The build job also assembles the deb, rpm and tgz packages, so changes to
        # their definitions and to the packaging script have to schedule a rebuild.
        "./packages",
        "./utils/list-licenses",
        "./utils/self-extracting-executable",
    ],
    with_git_submodules=True,
)

fast_test_digest_config = Job.CacheDigestConfig(
    include_paths=[
        "./ci/jobs/fast_test.py",
        "./ci/jobs/scripts/clickhouse_proc.py",
        "./ci/jobs/scripts/server_cleanup.py",
        "./tests/queries/0_stateless/",
        "./tests/config/",
        "./tests/clickhouse-test",
        "./src",
        "./contrib/",
        "./.gitmodules",
        "./CMakeLists.txt",
        "./PreLoad.cmake",
        "./cmake",
        "./base",
        "./programs",
        "./rust",
    ],
)

# The Darwin fast test additionally consumes the Darwin skip list and its wrapper
# script, so changes to either must schedule the job (the shared digest above does
# not cover them).
darwin_fast_test_digest_config = Job.CacheDigestConfig(
    include_paths=fast_test_digest_config.include_paths
    + ["./ci/defs/darwin.skip", "./ci/jobs/scripts/fast_test_darwin.sh"],
)

common_build_job_config = Job.Config(
    name=JobNames.BUILD,
    runs_on=[],  # from parametrize()
    requires=[],
    command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
    run_in_docker=BINARY_DOCKER_COMMAND,
    timeout=3600 * 4,
    digest_config=build_digest_config,
    needs_submodules=True,
)

common_ft_job_config = Job.Config(
    name=JobNames.STATELESS,
    runs_on=[],  # from parametrize
    command='python3 ./ci/jobs/functional_tests.py --options "{PARAMETER}"',
    # some tests can be flaky due to very slow disks - use tmpfs for temporary ClickHouse files
    # --cap-add=SYS_PTRACE and --privileged for gdb in docker
    # --root/--privileged/--cgroupns=host is required for clickhouse-test --memory-limit
    # --ulimit nofile is raised so that azurite-rs (the in-process Azure Blob
    # Storage emulator) does not run out of file descriptors under parallel load
    run_in_docker=f"clickhouse/stateless-test+--memory={LIMITED_MEM}+--cgroupns=host+--cap-add=SYS_PTRACE+--privileged+--security-opt seccomp=unconfined+--ulimit nofile=1048576:1048576+--tmpfs /tmp/clickhouse:mode=1777+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/etc/clickhouse-server1:/etc/clickhouse-server1+--volume=./ci/tmp/etc/clickhouse-server2:/etc/clickhouse-server2+--volume=./ci/tmp/var/log:/var/log+root",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_tests.py",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/server_cleanup.py",
            "./ci/jobs/scripts/functional_tests_results.py",
            "./ci/jobs/scripts/log_export.py",
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
            "./tests/queries",
            "./tests/clickhouse-test",
            "./tests/config",
            "./tests/*.txt",
            "./ci/docker/stateless-test",
            "./ci/jobs/scripts/functional_tests/setup_seaweedfs.sh",
        ],
    ),
    result_name_for_cidb="Tests",
    timeout=int(3600 * 2.5),
)

common_unit_test_job_config = Job.Config(
    name=JobNames.UNITTEST,
    runs_on=[],  # from parametrize()
    command="python3 ./ci/jobs/unit_tests_job.py --gtest_filter=-FunctionsStress.*",
    run_in_docker="clickhouse/test-base+--privileged",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/unit_tests_job.py",
            "./src/Functions/tests/gtest_functions_stress.cpp",
        ],
    ),
)

common_stress_job_config = Job.Config(
    name=JobNames.STRESS,
    runs_on=[],  # from parametrize()
    command="python3 ./ci/jobs/stress_job.py",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./tests/queries/0_stateless/",
            "./ci/jobs/stress_job.py",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/stress/stress.py",
            "./tests/clickhouse-test",
            "./tests/config",
            "./tests/*.txt",
            "./tests/docker_scripts/",
            "./ci/docker/stress-test",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/log_parser.py",
            # `stress_runner.sh` exports the system logs through
            # `clickhouse_proc.py logs_export_*`. The `ci_logs_sender` user is
            # covered by `./tests/config` above.
            "./ci/jobs/scripts/log_export.py",
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
        ],
    ),
    timeout=3600 * 3,
)
common_integration_test_job_config = Job.Config(
    name=JobNames.INTEGRATION,
    runs_on=[],  # from parametrize
    command="python3 ./ci/jobs/integration_test_job.py --options '{PARAMETER}'",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/integration_test_job.py",
            "./ci/jobs/scripts/integration_tests_configs.py",
            "./ci/jobs/scripts/job_hooks/promql_compliance_upload_hook.py",
            "./ci/jobs/scripts/job_hooks/promql_compliance_s3.py",
            "./ci/jobs/promql_compliance_job.py",
            "./ci/jobs/scripts/job_hooks/promql_compliance_comment_hook.py",
            "./tests/integration/",
            "./ci/docker/integration",
            "./ci/jobs/scripts/docker_in_docker.sh",
            "./ci/jobs/scripts/prefetch-integration-test-images",
        ],
    ),
    # Under `--cgroupns=host` the nested daemon parents its containers at the host's `/docker`,
    # a sibling of this cgroup, so `--memory` bounds only the harness. Explicit because the
    # daemon default is configurable and `host` would silently restore that leak.
    run_in_docker=f"clickhouse/integration-tests-runner+root+--memory={LIMITED_MEM}+--privileged+--dns-search='.'+--security-opt seccomp=unconfined+--cap-add=SYS_PTRACE+{docker_sock_mount}+--volume=clickhouse_integration_tests_volume:/var/lib/docker+--cgroupns=private+--ulimit nofile=262144:262144{integration_dind_env}",
    post_hooks=[
        "python3 ci/jobs/scripts/job_hooks/docker_volume_clean_up_hook.py",
        "python3 ci/jobs/scripts/job_hooks/promql_compliance_upload_hook.py",
    ],
)


class JobConfigs:
    style_check = Job.Config(
        name=JobNames.STYLE_CHECK,
        runs_on=RunnerLabels.ARM_TINY,
        command="python3 ./ci/jobs/check_style.py",
        run_in_docker="clickhouse/style-test",
        enable_commit_status=True,
    )
    code_review = Job.Config(
        name=JobNames.CODE_REVIEW,
        runs_on=RunnerLabels.ARM_TINY,
        command="python3 ./ci/jobs/copilot_review_job.py --codex",
        allow_failure=True,
        enable_gh_auth=True,
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/set_sync_status_awaiting_hook.py"
        ],
    )
    fast_test = Job.Config(
        name=JobNames.FAST_TEST,
        runs_on=RunnerLabels.AMD_LARGE,
        command="python3 ./ci/jobs/fast_test.py",
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker="clickhouse/fasttest+--network=host+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/var/log:/var/log+--volume=.:/ClickHouse",
        digest_config=fast_test_digest_config,
        result_name_for_cidb="Tests",
        needs_submodules=True,
    )
    darwin_fast_test_jobs = Job.Config(
        name="Fast test",
        runs_on=None,  # from parametrize()
        # macOS needs 127.0.0.2+ aliased on lo0 (it does not auto-route 127.0.0.0/8)
        # so remote()/cluster() tests are reachable, and the aliases must be removed
        # afterwards or the reused runner leaks them into later jobs. That setup and
        # fail-closed teardown live in a wrapper script (not pre/post hooks, whose
        # exit codes praktika does not propagate to job status). The script path is
        # the whole command: an inlined shell command tripped str.format() braces and
        # the run-command file-path validator. See ci/jobs/scripts/fast_test_darwin.sh.
        command="./ci/jobs/scripts/fast_test_darwin.sh",
        digest_config=darwin_fast_test_digest_config,
        result_name_for_cidb="Tests",
        pre_hooks=[
            "sudo rm -rf /Library/Logs/DiagnosticReports/*",
        ],
        post_hooks=[
            # Timeout safety net only: a timed-out command is killed before its
            # teardown runs, so drop any leaked aliases here (best-effort: a hook
            # cannot fail the job, and a timed-out job already fails).
            'for i in $(seq 2 16); do sudo ifconfig lo0 -alias 127.0.0.$i 2>/dev/null || true; done',
            "python3 ./ci/jobs/scripts/job_hooks/clickhouse_test_cleanup_hook.py",
            "sudo rm -rf /Users/ec2-user/actions-runner/_work/ClickHouse/ClickHouse/ci/tmp/run* /System/Volumes/Data/System/Library/Caches/com.apple.coresymbolicationd/data /System/Volumes/Data/private/var/db/diagnostics/*",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            runs_on=RunnerLabels.MACOS_ARM_SMALL,
            requires=[ArtifactNames.CH_ARM_DARWIN_BIN],
        ),
    )
    tidy_build_arm_jobs = common_build_job_config.parametrize(
        Job.ParamSet(
            parameter=BuildTypes.ARM_TIDY,
            provides=[],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    tidy_build_amd_jobs = common_build_job_config.parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_TIDY,
            provides=[],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_DEBUG,
            provides=[ArtifactNames.CH_AMD_DEBUG],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_ASAN_UBSAN,
            provides=[
                ArtifactNames.CH_AMD_ASAN_UBSAN,
                ArtifactNames.UNITTEST_AMD_ASAN_UBSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_TSAN,
            provides=[
                ArtifactNames.CH_AMD_TSAN,
                ArtifactNames.UNITTEST_AMD_TSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_MSAN,
            provides=[
                ArtifactNames.CH_AMD_MSAN,
                ArtifactNames.UNITTEST_AMD_MSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_BINARY,
            provides=[ArtifactNames.CH_AMD_BINARY],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_DEBUG,
            provides=[ArtifactNames.CH_ARM_DEBUG],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_ASAN_UBSAN,
            provides=[ArtifactNames.CH_ARM_ASAN_UBSAN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_TSAN,
            provides=[ArtifactNames.CH_ARM_TSAN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_MSAN,
            provides=[ArtifactNames.CH_ARM_MSAN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_BINARY,
            provides=[ArtifactNames.CH_ARM_BINARY],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    coverage_build_jobs = common_build_job_config.parametrize(
        Job.ParamSet(
            parameter=BuildTypes.LLVM_COVERAGE_BUILD,
            provides=[
                ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD,
            ],
            runs_on=RunnerLabels.AMD_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.PER_TEST_COVERAGE,
            provides=[
                ArtifactNames.CH_AMD_PER_TEST_COVERAGE_BUILD,
            ],
            runs_on=RunnerLabels.AMD_LARGE,
        ),
    )
    release_build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_RELEASE,
            provides=[
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
            timeout=3 * 3600,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_RELEASE,
            provides=[
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    release_build_jobs_with_examples = [
        job.set_command(f"{job.command} --build-examples").set_provides(
            ArtifactNames.CLICKHOUSE_EXAMPLES
        )
        if f"({BuildTypes.ARM_RELEASE})" in job.name
        else job
        for job in release_build_jobs
    ]
    cfi_build_job = common_build_job_config.parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_CFI,
            provides=[ArtifactNames.CH_AMD_CFI],
            runs_on=RunnerLabels.ARM_LARGE,
            timeout=4 * 3600,
        ),
    )
    cfi_integration_jobs = common_integration_test_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_cfi, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_CFI],
            )
            for total_batches in (4,)
            for batch in range(1, total_batches + 1)
        ]
    )
    cfi_stress_job = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_cfi",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_CFI],
        ),
    )
    # sccache-warmup builds (MasterCI only): compile amd_release / arm_release
    # with the PR release builds' cmake flags (see PR_CACHE_WARMUP_BUILD_TYPES
    # in build_clickhouse.py) while keeping the shared sccache read-write. This
    # populates the cache so that read-only PR release builds get cache hits.
    # They provide no artifacts. The build profile hook runs so that the
    # "Build profile diff" check gets a master baseline built with the PR
    # flags - the arm warmup's object sizes and compile traces are the only
    # master data directly comparable to a PR build (official master builds
    # keep debug symbols, PR builds strip them). The hook uploads only for the
    # arm variant (see _PROFILED_BUILDS in build_profile_hook.py).
    sccache_warmup_build_jobs = [
        job.set_command(f"{job.command} --build-examples")
        if f"({BuildTypes.ARM_RELEASE_PR_CACHE_WARMUP})" in job.name
        else job
        for job in common_build_job_config.set_post_hooks(
            post_hooks=[
                "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
            ],
        ).parametrize(
            Job.ParamSet(
                parameter=BuildTypes.AMD_RELEASE_PR_CACHE_WARMUP,
                runs_on=RunnerLabels.ARM_LARGE,
                timeout=3 * 3600,
            ),
            Job.ParamSet(
                parameter=BuildTypes.ARM_RELEASE_PR_CACHE_WARMUP,
                runs_on=RunnerLabels.ARM_LARGE,
            ),
        )
    ]
    extra_validation_build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        # Job.ParamSet(
        #     parameter=BuildTypes.ARM_TSAN,
        #     provides=[
        #         ArtifactNames.CH_ARM_TSAN,
        #     ],
        #     runs_on=RunnerLabels.ARM_LARGE,
        # ),
    )
    special_build_jobs = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_DARWIN,
            provides=[
                ArtifactNames.CH_AMD_DARWIN_BIN,
                ArtifactNames.CH_AMD_DARWIN_PLAIN,
            ],
            runs_on=RunnerLabels.AMD_LARGE,  # cannot crosscompile on arm
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            provides=[
                ArtifactNames.CH_ARM_DARWIN_BIN,
                ArtifactNames.CH_ARM_DARWIN_PLAIN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_V80COMPAT,
            provides=[ArtifactNames.CH_ARM_V80COMPAT],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_FREEBSD,
            provides=[ArtifactNames.CH_AMD_FREEBSD],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.PPC64LE,
            provides=[ArtifactNames.CH_PPC64LE],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_COMPAT,
            provides=[ArtifactNames.CH_AMD_COMPAT],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_MUSL,
            provides=[ArtifactNames.CH_AMD_MUSL],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.RISCV64,
            provides=[ArtifactNames.CH_RISCV64],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.S390X,
            provides=[ArtifactNames.CH_S390X],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.LOONGARCH64,
            provides=[ArtifactNames.CH_LOONGARCH64],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.WASM64,
            provides=[ArtifactNames.CH_WASM64],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_FUZZERS,
            provides=[],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    # The standalone WebAssembly build of the SQL parser (utils/wasm-parser). It cross-compiles to
    # `wasm32-wasip1` with a wasi-sdk toolchain, which cannot be mixed into a tree configured for
    # the host, so it is a CMake project of its own driven by its own script in its own image -
    # not a `BuildTypes` entry in the `Build` matrix, which `binary-builder` and
    # `build_clickhouse.py` serve. The two post hooks the other build jobs carry are left off for
    # the same reason: neither the master-head binary nor the build profile has a counterpart here.
    #
    # Nothing else in CI compiles this module, so this job is also what notices when it stops
    # compiling - which it did within a day of being merged, twice over.
    wasm_parser_build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/build_wasm_parser.py",
        run_in_docker="clickhouse/wasm-builder",
        timeout=2 * 3600,
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/build_wasm_parser.py",
                "./utils/wasm-parser",
                # The closure the project names is the parser and everything it reaches, which
                # spans most of `src` and `base` and a dozen contrib libraries. Nothing narrower
                # than the build digest's own source paths bounds it.
                "./src",
                "./base",
                "./contrib/",
                "./.gitmodules",
            ],
            with_git_submodules=True,
        ),
        needs_submodules=True,
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.WASM_PARSER,
            provides=[ArtifactNames.CH_WASM_PARSER],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
    )
    install_check_jobs = Job.Config(
        name=JobNames.INSTALL_TEST,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/install_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/install_check.py",
                "./ci/docker/install",
                "./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py",
            ],
        ),
        timeout=900,
        # Unpacking the packages needs ~4.4 GB, so reclaim another job's leftover
        # images before installing, not just afterwards. Best-effort: praktika does
        # not propagate a hook's exit code to the job status.
        pre_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.AMD_TINY,
            requires=[
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
            ],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.ARM_TINY,
            requires=[
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
            ],
        ),
    )
    install_check_master_jobs = Job.Config(
        name=JobNames.INSTALL_TEST,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/install_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/install_check.py",
                "./ci/docker/install",
                "./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py",
            ],
        ),
        timeout=900,
        # See install_check_jobs above.
        pre_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.AMD_TINY,
            requires=[
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
                ArtifactNames.CH_AMD_RELEASE,
            ],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.ARM_TINY,
            requires=[
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
                ArtifactNames.CH_ARM_RELEASE,
            ],
        ),
    )
    stateless_tests_flaky_pr_jobs = common_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_asan_ubsan, flaky check",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan, flaky check",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan, flaky check",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="amd_debug, flaky check",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
    )
    stateless_tests_flaky_mq_jobs = common_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_binary, flaky check",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_BINARY],
        ),
    )
    stateless_tests_targeted_pr_jobs = common_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="arm_asan_ubsan, targeted",
            runs_on=RunnerLabels.ARM_LARGE,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
    )
    # Most sanitizer flavors of the functional tests for pull requests. They run only
    # the tests selected for the change (`selected tests`, see
    # `SELECTED_TESTS_OPTION` in `ci/jobs/functional_tests.py`) and replace the
    # full-suite sanitizer jobs of `functional_tests_jobs`, which the master
    # workflow keeps running in every flavor. What is left in a pull request is
    # the full suite in the debug and plain binary flavors, plus the stress
    # tests, which run the functional tests under every sanitizer with heavy
    # concurrency and randomized settings and find more than a plain functional
    # run does. See ClickHouse/ClickHouse#114725.
    #
    # The selection is a few hundred tests, so the batches of the full-suite jobs
    # are collapsed into a single job per flavor. The runner labels and the
    # timeout are kept as they are for the corresponding full-suite jobs: the
    # test runner sizes its worker pool from the CPU count, and a sanitizer
    # flavor that needs a large-memory runner for the full suite needs it for a
    # subset as well. If test selection cannot be fetched, the job fails instead
    # of silently running a weaker unbatched fallback configuration.
    # The selection is computed from PR-local state (including failed tests
    # from earlier jobs).
    selected_ft_job_config = common_ft_job_config.copy()
    stateless_tests_selected_pr_jobs = selected_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_asan_ubsan, distributed plan, parallel, selected tests",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_asan_ubsan, db disk, distributed plan, sequential, selected tests",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan, parallel, selected tests",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan, sequential, selected tests",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan, s3 storage, parallel, selected tests",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan, s3 storage, sequential, selected tests",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
    )
    # --root/--privileged/--cgroupns=host is required for clickhouse-test --memory-limit
    #
    # Per-arch Bugfix Validation Check (functional tests).
    #
    # Each variant (amd64, aarch64) runs the new/modified test on master HEAD
    # and on the PR, then inverts the test status. The runner sets one of three
    # top-level statuses (see `invert_bugfix_validation_status` in
    # `ci/jobs/functional_tests.py`):
    #   * `OK` / `XFAIL`: bug reproduced on master HEAD AND fixed on the PR
    #                     for this arch (validated)
    #   * `SKIPPED`: bug did not reproduce on master HEAD on this arch
    #                (no-repro: another arch can still validate)
    #   * `ERROR` / `FAIL`: infrastructure error / inconclusive run (no signal)
    #
    # Each per-arch job has `allow_failure=True` so a genuine `ERROR` does NOT
    # block PR merge on its own. The merge-blocking decision is centralized in
    # the `new_tests_check.py` workflow post-hook, which uses strict
    # `is_success()`: validation passes iff AT LEAST ONE per-arch job is
    # `OK`/`XFAIL`. `SKIPPED`/`ERROR`/`FAIL` per-arch jobs do NOT count.
    #
    # Rationale: some bug fixes are architecture-specific (e.g. SSE2/AVX-only
    # on x86, NEON-only on aarch64). With the previous monolithic single-arch
    # check, those PRs would fail Bugfix validation on the "wrong" arch.
    # Splitting per-arch + aggregating in the post-hook fixes that.
    bugfix_validation_ft_pr_jobs = Job.Config(
        name=JobNames.BUGFIX_VALIDATE,
        runs_on=None,  # set per ParamSet
        command="python3 ./ci/jobs/functional_tests.py --options BugfixValidation",
        # some tests can be flaky due to very slow disks - use tmpfs for temporary ClickHouse files
        run_in_docker="clickhouse/stateless-test+--network=host+--privileged+--cgroupns=host+root+--security-opt seccomp=unconfined+--ulimit nofile=1048576:1048576+--tmpfs /tmp/clickhouse:mode=1777",
        # No digest_config: the Bugfix Validation verdict is intentionally NOT
        # cacheable. Its inputs are not captured by any set of repository files -
        # it depends on (1) the PR's source fix and (2) the master-HEAD binary
        # that the runner downloads at run time from a recent master commit (see
        # `bugfix_validation.find_master_builds`), and master HEAD advances
        # independently of the PR. With a digest, a `SKIPPED` no-repro verdict is
        # pushed as a cache-success record (the cache uses `Result.is_ok`, which
        # treats SKIPPED as success) and then reused on any later commit whose
        # test content hashes the same - even after the fix or master HEAD
        # changed. The job never re-runs, so `new_tests_check.py` fails with "No
        # per-arch Bugfix Validation job validated the bug". Leaving the job
        # uncached makes it re-run on every eligible commit; it stays gated to
        # bug-fix PRs with test changes by `filter_job.py` and runs only the
        # changed tests, so this is cheap. See ClickHouse/ClickHouse#109229.
        digest_config=None,
        result_name_for_cidb="Tests",
    ).set_allow_failure(True).parametrize(
        Job.ParamSet(
            parameter="functional tests, amd64",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
        ),
        Job.ParamSet(
            parameter="functional tests, aarch64",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
        ),
    )
    lightweight_functional_tests_job = Job.Config(
        name="Quick functional tests",
        command="python3 ./ci/jobs/clickhouse_light.py --path ./ci/tmp/clickhouse",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/clickhouse_light.py",
                "./ci/jobs/scripts/server_cleanup.py",
                "./ci/jobs/queries",
            ],
        ),
        requires=[ArtifactNames.CH_AMD_DEBUG],
        runs_on=RunnerLabels.AMD_SMALL,
    )
    functional_tests_jobs = common_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_asan_ubsan, distributed plan, parallel",
            # `--distributed-plan` fans each query across local parallel replicas,
            # multiplying the server-side memory of every in-flight query, so the
            # aggregate RSS of the parallel suite's co-scheduled queries is heavier
            # than a normal ASan run and overruns the sanitizer memory cap on the
            # default 64 GiB runner. Use a LARGE runner (same 32 vCPU as MEDIUM_CPU,
            # but 128 GiB instead of 64 GiB RAM) so the suite keeps full concurrency
            # and the default timeout instead of cutting workers - which barely
            # moved peak RSS and only made the job slower.
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, db disk, distributed plan, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL_MEM,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
            for total_batches in (3,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_llvm_coverage, old analyzer, s3 storage, DBReplicated, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
                requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
                provides=[
                    ArtifactNames.LLVM_COVERAGE_FILE
                    + f"_ft_old_s3_db_repl_parallel_{batch}"
                ],
            )
            for total_batches in (LLVM_FT_OLD_S3_DB_REPL_NUM_BATCHES,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_llvm_coverage, old analyzer, s3 storage, DBReplicated, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL,
                requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
                provides=[
                    ArtifactNames.LLVM_COVERAGE_FILE
                    + f"_ft_old_s3_db_repl_sequential_{batch}"
                ],
            )
            for total_batches in (LLVM_FT_OLD_S3_DB_REPL_SEQUENTIAL_NUM_BATCHES,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="amd_llvm_coverage, ParallelReplicas, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + "_ft_s3_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, ParallelReplicas, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + "_ft_s3_sequential"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, AsyncInsert, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + "_ft_s3_async_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, AsyncInsert, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + "_ft_s3_async_sequential"],
        ),
        Job.ParamSet(
            parameter="amd_debug, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM_CPU,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_debug, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_tsan, parallel",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_LARGE,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (3,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL_MEM,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="amd_debug, distributed plan, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_debug, distributed plan, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, s3 storage, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (3,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, s3 storage, sequential, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL_MEM,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="arm_binary, parallel",
            runs_on=RunnerLabels.ARM_MEDIUM_CPU,
            requires=[ArtifactNames.CH_ARM_BINARY],
        ),
        Job.ParamSet(
            parameter="arm_binary, sequential",
            runs_on=RunnerLabels.ARM_SMALL,
            requires=[ArtifactNames.CH_ARM_BINARY],
        ),
    )
    functional_tests_jobs_coverage = common_ft_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"{BuildTypes.PER_TEST_COVERAGE}, per_test_coverage, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_SMALL,
                requires=[ArtifactNames.CH_AMD_PER_TEST_COVERAGE_BUILD],
            )
            for total_batches in (8,)
            for batch in range(1, total_batches + 1)
        ]
    )
    functional_tests_jobs_azure = common_ft_job_config.set_allow_failure(
        True
    ).parametrize(
        Job.ParamSet(
            parameter="arm_asan_ubsan, azure, parallel",
            runs_on=RunnerLabels.ARM_LARGE,  # ~2h on medium
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan, azure, sequential",
            runs_on=RunnerLabels.ARM_SMALL_MEM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
    )
    # Per-arch Bugfix Validation Check (integration tests). See the rationale
    # and status-model comment above for `bugfix_validation_ft_pr_jobs`. Each
    # per-arch variant has `allow_failure=True` so a genuine `ERROR` doesn't
    # block PR merge directly; the `new_tests_check.py` workflow post-hook
    # centralizes the merge decision via strict `is_success()` (validated iff
    # at least one per-arch job is `OK`/`XFAIL`).
    bugfix_validation_it_jobs = (
        common_integration_test_job_config.set_name(JobNames.BUGFIX_VALIDATE)
        .set_command(
            "python3 ./ci/jobs/integration_test_job.py --options BugfixValidation"
        )
        .set_allow_failure(True)
    )
    # No digest_config: the Bugfix Validation verdict is intentionally NOT
    # cacheable - it depends on the PR's source fix and the run-time-selected
    # master-HEAD binary, neither of which is a repository file, so no digest can
    # capture its true inputs. The `common_integration_test_job_config` carries a
    # digest; clear the (deep-copied) one here so a `SKIPPED` no-repro verdict is
    # never pushed as a cache-success record and reused on a later commit. See the
    # matching comment on `bugfix_validation_ft_pr_jobs` and
    # ClickHouse/ClickHouse#109229.
    bugfix_validation_it_jobs.digest_config = None
    bugfix_validation_it_jobs = bugfix_validation_it_jobs.parametrize(
        Job.ParamSet(
            parameter="integration tests, amd64",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
        ),
        Job.ParamSet(
            parameter="integration tests, aarch64",
            runs_on=RunnerLabels.ARM_SMALL_MEM,
        ),
    )
    # Builds the "before" unit_tests_dbms (merge-base + only the PR's unit-test file
    # changes) in-job, so it needs the binary-builder image and submodules. It does NOT
    # require the PR's UNITTEST_AMD_ASAN_UBSAN artifact: the "touched tests pass on the
    # PR binary" side is delegated to the regular `Unit tests (asan_ubsan)` job (see the
    # module docstring), so this job is not gated behind `build_amd_asan_ubsan` and
    # builds the "before" binary in parallel with the build matrix — matching the early
    # start of the functional/integration bugfix validators.
    bugfix_validation_ut_job = Job.Config(
        name=JobNames.BUGFIX_VALIDATE_UT,
        runs_on=RunnerLabels.AMD_LARGE,
        command="python3 ./ci/jobs/unit_tests_bugfix_validation_job.py",
        # The job both builds and RUNS the before-binary in this container. Running
        # `unit_tests_dbms` needs `io_uring` (the `silk` fiber runtime calls
        # `io_uring_queue_init_params` at startup), which Docker's default seccomp
        # profile blocks — without this the before-binary aborts before any test runs.
        # `--privileged` mirrors how the regular unit-test job runs the same binary
        # (`clickhouse/test-base+--privileged`).
        run_in_docker=BINARY_DOCKER_COMMAND + "+--privileged",
        needs_submodules=True,
        timeout=3600 * 4,
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/unit_tests_bugfix_validation_job.py",
                "./ci/jobs/build_clickhouse.py",
                "./src",
                "./contrib/",
                "./.gitmodules",
                "./CMakeLists.txt",
                "./PreLoad.cmake",
                "./cmake",
                "./base",
                "./programs",
                "./rust",
            ],
            with_git_submodules=True,
        ),
        result_name_for_cidb="Tests",
    ).set_allow_failure(True)
    # allow_failure: an inconclusive ERROR (e.g. the before-binary could not be compiled,
    # or crashed before any test ran) must NOT hard-block merge — "we couldn't determine"
    # is not a reason to block. Only a definitive FAIL (the added test passes on the
    # merge-base too, so it doesn't catch the bug) should block. Like the FT/IT bugfix
    # jobs, the merge decision is centralized in new_tests_check.py, which blocks the unit
    # case iff this job reported FAIL.
    _fuzzer_command = (
        "python3 ./ci/jobs/unit_tests_job.py --gtest_filter=FunctionsStress.*"
    )
    unittest_jobs = common_unit_test_job_config.parametrize(
        Job.ParamSet(
            parameter="asan_ubsan",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="tsan",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="msan",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="asan_ubsan, function_prop_fuzzer",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_ASAN_UBSAN],
            command=_fuzzer_command,
        ),
        Job.ParamSet(
            parameter="tsan, function_prop_fuzzer",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_TSAN],
            command=_fuzzer_command,
        ),
        Job.ParamSet(
            parameter="msan, function_prop_fuzzer",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_AMD_MSAN],
            command=_fuzzer_command,
        ),
    )
    stress_test_jobs = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_debug",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan, s3",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_TSAN],
        ),
        Job.ParamSet(
            parameter="arm_msan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_MSAN],
        ),
    )
    # might be heavy on azure - run only on master
    stress_test_azure_jobs = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="azure, amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="azure, amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
    )
    upgrade_test_jobs = Job.Config(
        name=JobNames.UPGRADE,
        runs_on=["from param"],
        command="python3 ./ci/jobs/upgrade_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/upgrade_job.py",
                "./ci/jobs/stress_job.py",
                "./ci/jobs/scripts/stress/stress.py",
                "./tests/docker_scripts/",
                "./ci/docker/stress-test",
                "./ci/jobs/scripts/log_parser.py",
                # upgrade_runner.sh symlinks and runs both of these, and ./ci does
                # not cover ./tests/ci.
                "./tests/ci/get_previous_release_tag.py",
                "./tests/ci/download_release_packages.py",
            ]
        ),
        timeout=3600 * 2,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_RELEASE],
        ),
    )
    # why it's master only?
    integration_test_asan_master_jobs = common_integration_test_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, db disk, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
            for total_batches in (4,)
            for batch in range(1, total_batches + 1)
        ]
    )
    integration_test_jobs_required = common_integration_test_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, db disk, old analyzer, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"arm_binary, distributed plan, {batch}/{total_batches}",
                runs_on=RunnerLabels.ARM_MEDIUM,
                requires=[ArtifactNames.CH_ARM_BINARY],
            )
            for total_batches in (4,)
            for batch in range(1, total_batches + 1)
        ],
    )
    integration_test_jobs_non_required = common_integration_test_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (8,)
            for batch in range(1, total_batches + 1)
        ],
    )
    integration_test_asan_flaky_pr_jobs = (
        common_integration_test_job_config.parametrize(
            Job.ParamSet(
                parameter="amd_asan_ubsan, flaky",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
        )
    )

    build_llvm_coverage_job = common_build_job_config.set_post_hooks(
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.LLVM_COVERAGE_BUILD,
            provides=[
                ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD,
                ArtifactNames.UNITTEST_LLVM_COVERAGE,
            ],
            runs_on=RunnerLabels.AMD_LARGE,
        ),
    )

    unittest_llvm_coverage_job = common_unit_test_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_llvm_coverage",
            runs_on=RunnerLabels.AMD_LARGE,
            requires=[ArtifactNames.UNITTEST_LLVM_COVERAGE],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE],
        ),
    )

    functional_test_llvm_coverage_jobs = common_ft_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_llvm_coverage, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
                provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_{batch}"],
            )
            for total_batches in (LLVM_FT_NUM_BATCHES,)
            for batch in range(1, total_batches + 1)
        ]
    )

    integration_test_llvm_coverage_jobs = (
        common_integration_test_job_config.parametrize(
            *[
                Job.ParamSet(
                    parameter=f"amd_llvm_coverage, {batch}/{total_batches}",
                    runs_on=RunnerLabels.AMD_MEDIUM,
                    requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
                    provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_it_{batch}"],
                )
                for total_batches in (LLVM_IT_NUM_BATCHES,)
                for batch in range(1, total_batches + 1)
            ],
        )
    )

    # Jobs that run only the tests normally disabled under LLVM coverage.
    # They use a regular binary (no coverage instrumentation) since these
    # tests are too slow or problematic under coverage.
    functional_test_excluded_from_llvm_job = common_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_binary_excluded_from_llvm",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_BINARY],
        ),
    )

    integration_test_excluded_from_llvm_job = (
        common_integration_test_job_config.parametrize(
            Job.ParamSet(
                parameter="amd_binary_excluded_from_llvm",
                runs_on=RunnerLabels.AMD_MEDIUM,
                requires=[ArtifactNames.CH_AMD_BINARY],
            ),
        )
    )

    integration_test_targeted_pr_jobs = common_integration_test_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_asan_ubsan, targeted",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        )
    )
    # Keeper stress job config — shared by PR and nightly workflows.
    # Mode (PR vs nightly faults vs nightly no-faults) is determined inside the job
    # script via Info().pr_number and Info().workflow_name.
    keeper_stress_job = Job.Config(
        name="Keeper Stress",
        runs_on=RunnerLabels.ARM_LARGE,
        command="python3 ./ci/jobs/keeper_stress_job.py",
        run_in_docker=(
            f"clickhouse/integration-tests-runner+root+--memory={KEEPER_DIND_MEM}+--privileged+--dns-search='.'+"
            f"--security-opt seccomp=unconfined+--cap-add=SYS_PTRACE+{docker_sock_mount}+--volume=clickhouse_integration_tests_volume:/var/lib/docker+--ulimit nofile=262144:262144"
        ),
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/keeper_stress_job.py",
                "./ci/jobs/scripts/docker_in_docker.sh",
                "./tests/stress/keeper/",
                "./tests/integration/helpers/",
                "./src/Coordination/",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_BINARY],
        result_name_for_cidb="Keeper Stress",
        timeout=24 * 3600,
        post_hooks=["python3 ./ci/jobs/scripts/ingest_keeper_metrics.py"],
    )
    compatibility_test_jobs = Job.Config(
        name=JobNames.COMPATIBILITY,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/compatibility_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/compatibility_check.py",
            ],
        ),
        # Shares the style-checker runners with Install packages and leaves ~4 GB of docker
        # residue per run, which is what the next job on that runner inherits.
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.AMD_TINY,
            requires=[ArtifactNames.DEB_AMD_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.ARM_TINY,
            requires=[ArtifactNames.DEB_ARM_RELEASE],
        ),
    )
    ast_fuzzer_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=[],  # from parametrize()
        command='python3 ./ci/jobs/ast_fuzzer_job.py "{PARAMETER}"',
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/log_export.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                # Copied into the server config by `run-fuzzer.sh`
                "./tests/config/users.d/ci_logs_sender.yaml",
                "./ci/jobs/scripts/fuzzer/",
                "./tests/config/config.d/core_dump.yaml",
                "./ci/docker/fuzzer",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="amd_release, oracle",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_RELEASE],
        ),
    )
    ast_fuzzer_targeted_pr_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=[],  # from parametrize()
        command='python3 ./ci/jobs/ast_fuzzer_job.py "{PARAMETER}"',
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/find_symbols.py",
                "./ci/jobs/scripts/find_tests.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/log_export.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                # Copied into the server config by `run-fuzzer.sh`
                "./tests/config/users.d/ci_logs_sender.yaml",
                "./ci/jobs/scripts/fuzzer/",
                "./tests/config/config.d/core_dump.yaml",
                "./ci/docker/fuzzer",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug, targeted",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_debug, targeted, old_compatibility",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),

    )
    buzz_fuzzer_jobs = Job.Config(
        name=JobNames.BUZZHOUSE,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/buzzhouse_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/buzzhouse_job.py",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/log_export.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                # Copied into the server config by `run-fuzzer.sh`
                "./tests/config/users.d/ci_logs_sender.yaml",
                "./ci/jobs/scripts/fuzzer/",
                "./tests/config/config.d/core_dump.yaml",
                "./ci/docker/fuzzer",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.ARM_MEDIUM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_MSAN],
        ),
    )
    performance_comparison_with_master_head_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["#from param"],
        command='python3 ./ci/jobs/performance_tests.py --test-options "{PARAMETER}"',
        # TODO: switch to stateless-test image
        run_in_docker="clickhouse/performance-comparison",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/performance/",
                "./ci/jobs/scripts/perf/",
                "./ci/jobs/performance_tests.py",
                "./ci/docker/performance-comparison",
                # Both servers export their system logs to the CI Logs cluster
                "./ci/jobs/scripts/log_export.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./tests/config/users.d/ci_logs_sender.yaml",
            ],
        ),
        timeout=2 * 3600,
        result_name_for_cidb="Tests",
    ).parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_release, master_head, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_AMD,
                requires=[ArtifactNames.CH_AMD_RELEASE],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"arm_release, master_head, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_ARM,
                requires=[ArtifactNames.CH_ARM_RELEASE],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
    )
    performance_comparison_with_release_base_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["#from param"],
        command='python3 ./ci/jobs/performance_tests.py --test-options "{PARAMETER}"',
        # TODO: switch to stateless-test image
        run_in_docker="clickhouse/performance-comparison",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/performance/",
                "./ci/jobs/scripts/perf/",
                "./ci/jobs/performance_tests.py",
                "./ci/docker/performance-comparison",
                # Both servers export their system logs to the CI Logs cluster
                "./ci/jobs/scripts/log_export.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./tests/config/users.d/ci_logs_sender.yaml",
            ],
        ),
        timeout=2 * 3600,
        result_name_for_cidb="Tests",
    ).parametrize(
        *[
            Job.ParamSet(
                parameter=f"arm_release, release_base, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_ARM,
                requires=[ArtifactNames.CH_ARM_RELEASE],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ]
    )
    clickbench_jobs = Job.Config(
        name=JobNames.CLICKBENCH,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="python3 ./ci/jobs/clickbench.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/clickbench.py",
                # ClickBench starts the server via `ClickHouseService`, which
                # clears leftover processes through `server_cleanup.py`. Track
                # both so changes to the shared start path reschedule the job.
                "./ci/jobs/scripts/clickhouse_proc.py",
                "./ci/jobs/scripts/server_cleanup.py",
                "./ci/jobs/scripts/clickbench/",
                "./ci/jobs/scripts/clickhouse_service.py",
                "./ci/jobs/scripts/log_export.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/praktika/result.py",
                "./tests/config/users.d/ci_logs_sender.yaml",
            ],
        ),
        run_in_docker="clickhouse/stateless-test+--shm-size=16g+--network=host",
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_RELEASE,
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_RELEASE],
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_RELEASE,
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_RELEASE],
        ),
    )
    docs_job_mintlify = Job.Config(
        name=JobNames.DOCS_MINTLIFY,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_job_mintlify.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./docs",
                "./ci/jobs/docs_job_mintlify.py",
                "./ci/jobs/scripts/docs",
                "./utils/generate-async-metrics-docs",
                "./utils/generate-system-tables-docs",
                # The source of truth for the generated Open source changelog
                # page, so a change to it alone must still run this job.
                "./CHANGELOG.md",
            ],
            # These files are internal inputs or contributor documentation, not
            # pages published by Mintlify.
            exclude_paths=[
                "./docs/README.md",
                "./docs/_templates/",
                "./docs/_includes/",
                "./docs/changelog_entry_guidelines.md",
                "./docs/changelogs/",
            ],
        ),
        run_in_docker="clickhouse/docs-builder",
    )
    docker_server = Job.Config(
        name=JobNames.DOCKER_SERVER,
        runs_on=RunnerLabels.AMD_TINY,
        # --apt-mirror-region points apt at the in-region AWS Ubuntu mirror; the
        # runners are in us-east-1, where Canonical's mirrors are often unreachable.
        command="python3 ./ci/jobs/docker_server.py --tag-type head --allow-build-reuse --apt-mirror-region us-east-1",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/docker_server.py",
                "./docker/server",
                "./docker/keeper",
            ],
        ),
        requires=["Build (amd_release)", "Build (arm_release)"],
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    )
    docker_keeper = Job.Config(
        name=JobNames.DOCKER_KEEPER,
        runs_on=RunnerLabels.AMD_TINY,
        # --apt-mirror-region points apt at the in-region AWS Ubuntu mirror; the
        # runners are in us-east-1, where Canonical's mirrors are often unreachable.
        command="python3 ./ci/jobs/docker_server.py --tag-type head --allow-build-reuse --apt-mirror-region us-east-1",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/docker_server.py",
                "./docker/server",
                "./docker/keeper",
            ],
        ),
        requires=["Build (amd_release)", "Build (arm_release)"],
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    )
    # Both fuzzers run against two builds. The release build is the wrong-result
    # hunt: a sanitizer binary executes SQL 2-3x slower, so most of a 5h budget
    # would buy sanitizer coverage instead of SQL coverage (measured: 47-80
    # queries/s at 10 threads on arm_asan_ubsan). The arm_asan_ubsan build is kept
    # as its own job for what only it can find - memory errors and UB reached
    # through generated SQL.
    # `-e SLACK_WEBHOOK_CORE_QA`: the job alerts on new findings (see
    # ci/jobs/scripts/sqlancer_notify.py); without the secret it is a no-op.
    sqlancer_master_jobs = Job.Config(
        name=JobNames.SQLANCER,
        runs_on=[],  # from parametrize()
        command="./ci/jobs/sqlancer_job.sh",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqlancer_job.sh",
                "./ci/jobs/scripts/sqlancer_failures.py",
                "./ci/jobs/scripts/sqlancer_notify.py",
                "./ci/jobs/scripts/sqlancer_server_errors.sh",
                "./ci/docker/sqlancer-test",
            ],
        ),
        run_in_docker="clickhouse/sqlancer-test+-e SLACK_WEBHOOK_CORE_QA",
        # 5h sqlancer run (set in sqlancer_job.sh) plus server start/teardown.
        timeout=3600 * 5 + 1800,
    ).parametrize(
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
    )
    sqlancer_pp_jobs = Job.Config(
        name=JobNames.SQLANCER_PP,
        runs_on=[],  # from parametrize()
        command="./ci/jobs/sqlancer_pp_job.sh",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqlancer_pp_job.sh",
                "./ci/jobs/scripts/sqlancer_server_errors.sh",
                "./ci/docker/sqlancer-test",
            ],
        ),
        run_in_docker="clickhouse/sqlancer-test",
        timeout=3600,
    ).parametrize(
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
    )
    sqltest_master_job = Job.Config(
        name=JobNames.SQL_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/sqltest_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqltest_job.py",
                "./ci/jobs/scripts/server_cleanup.py",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=10800,
    )
    sqllogic_test_master_job = Job.Config(
        name=JobNames.SQL_LOGIC_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/sqllogic_test.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqllogic_test.py",
                "./ci/jobs/scripts/server_cleanup.py",
                "./tests/sqllogic/",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=10800,
    )
    docs_examples_job = Job.Config(
        name=JobNames.DOCS_EXAMPLES,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_examples_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/docs_examples_job.py",
                "./ci/jobs/scripts/server_cleanup.py",
                "./tests/docs_examples/",
                # The server of this job installs `programs/server/config.d` and
                # `programs/server/users.d` dereferenced, and most of their entries are symlinks
                # into `tests/config`, so the whole of it is an input of the job.
                "./tests/config/",
                "./programs/server/config.xml",
                "./programs/server/config.d/",
                "./programs/server/users.xml",
                "./programs/server/users.d/",
            ]
            # The examples are extracted from server source registrations and validate server
            # behavior, so every change that rebuilds the server must run this job too.
            + build_digest_config.include_paths,
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=3600,
    )
    sqlstorm_test_job = Job.Config(
        name=JobNames.SQL_STORM_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/sqlstorm_test.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqlstorm_test.py",
                "./ci/jobs/scripts/server_cleanup.py",
                "./ci/jobs/scripts/log_export.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                # Copied into the server config by `create_log_export_config`
                "./tests/config/users.d/ci_logs_sender.yaml",
                "./tests/sqlstorm/",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=10800,
    )
    jepsen_keeper = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.AMD_TINY,
        command="python3 ./ci/jobs/jepsen_check.py keeper",
        requires=["Build (amd_binary)"],
    )
    jepsen_server = Job.Config(
        name=JobNames.JEPSEN_SERVER,
        runs_on=RunnerLabels.AMD_TINY,
        command="python3 ./ci/jobs/jepsen_check.py server",
        requires=["Build (amd_binary)"],
    )
    libfuzzer_job = Job.Config(
        name=JobNames.LIBFUZZER_TEST,
        runs_on=RunnerLabels.ARM_MEDIUM,
        command="python3 ./ci/jobs/libfuzzer_test_check.py 'libFuzzer tests'",
        requires=[ArtifactNames.ARM_FUZZERS, ArtifactNames.FUZZERS_CORPUS],
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/libfuzzer_test_check.py"],
        ),
    )
    libfuzzer_corpus_minimization_job = Job.Config(
        name=JobNames.LIBFUZZER_CORPUS_MINIMIZATION,
        runs_on=RunnerLabels.ARM_MEDIUM,
        command=(
            "python3 ./ci/jobs/libfuzzer_test_check.py --minimize-only "
            "'libFuzzer corpus minimization'"
        ),
        requires=[ArtifactNames.ARM_FUZZERS, ArtifactNames.FUZZERS_CORPUS],
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/libfuzzer_test_check.py"],
        ),
    )
    collect_clickhouse_profiles_jobs = Job.Config(
        name=JobNames.COLLECT_CLICKHOUSE_PROFILES,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/collect_clickhouse_profiles.py",
        run_in_docker=BINARY_DOCKER_COMMAND,
        timeout=8 * 3600,
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/collect_clickhouse_profiles.py",
                "./ci/jobs/scripts/server_cleanup.py",
                "./cmake/profile_optimization.cmake",
                "./tests/performance/",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd64",
            runs_on=RunnerLabels.AMD_LARGE,
            provides=[
                ArtifactNames.CLICKHOUSE_PGO_PROFILE_AMD,
                ArtifactNames.CLICKHOUSE_BOLT_PROFILE_AMD,
            ],
        ),
        Job.ParamSet(
            parameter="aarch64",
            runs_on=RunnerLabels.ARM_LARGE,
            provides=[
                ArtifactNames.CLICKHOUSE_PGO_PROFILE_ARM,
                ArtifactNames.CLICKHOUSE_BOLT_PROFILE_ARM,
            ],
        ),
    )
    parser_memory_check_job = Job.Config(
        name=JobNames.PARSER_MEMORY_CHECK,
        runs_on=RunnerLabels.ARM_SMALL,
        run_in_docker="clickhouse/test-base",
        command="python3 ./ci/jobs/parser_memory_check.py",
        requires=[ArtifactNames.CLICKHOUSE_EXAMPLES],
        result_name_for_cidb="Tests",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/parser_memory_check.py",
                "./utils/parser-memory-profiler/",
            ],
        ),
    )
    toolchain_build_jobs = Job.Config(
        name=JobNames.BUILD_TOOLCHAIN,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/build_toolchain.py",
        run_in_docker=BINARY_DOCKER_COMMAND,
        timeout=8 * 3600,
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/build_toolchain.py"],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="amd64",
            runs_on=RunnerLabels.AMD_LARGE,
            provides=[ArtifactNames.TOOLCHAIN_PGO_BOLT_AMD],
        ),
        Job.ParamSet(
            parameter="aarch64",
            runs_on=RunnerLabels.ARM_LARGE,
            provides=[ArtifactNames.TOOLCHAIN_PGO_BOLT_ARM],
        ),
    )
    update_toolchain_dockerfile_job = Job.Config(
        name=JobNames.UPDATE_TOOLCHAIN_DOCKERFILE,
        runs_on=RunnerLabels.AMD_TINY,
        command="python3 ./ci/jobs/update_toolchain_dockerfile.py",
        enable_gh_auth=True,
    )
    vector_search_stress_job = Job.Config(
        name="Vector Search Stress",
        runs_on=RunnerLabels.ARM_LARGE_STORAGE,
        run_in_docker="clickhouse/performance-comparison",
        command="python3 ./ci/jobs/vector_search_stress_tests.py",
        timeout=6 * 3600,
    )
    # Compares the PR's arm_release build profile (binary and per-object sizes,
    # per-symbol sizes, compile and link time down to individual functions and
    # template instantiations) against the latest master build, and posts a PR
    # comment when the change is significant.
    build_profile_diff_job = Job.Config(
        name=JobNames.BUILD_PROFILE_DIFF,
        runs_on=RunnerLabels.ARM_SMALL,
        # stateless-test rather than test-base: the job reads the CI logs
        # cluster credentials from AWS SSM, and stateless-test is the test
        # image that ships the aws CLI.
        run_in_docker="clickhouse/stateless-test",
        requires=["Build (arm_release)"],
        command="python3 ./ci/jobs/build_profile_diff_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/build_profile_diff_job.py"],
        ),
        timeout=1800,
        enable_gh_auth=True,
    )
    llvm_coverage_job = Job.Config(
        name=JobNames.LLVM_COVERAGE,
        runs_on=RunnerLabels.AMD_SMALL,
        run_in_docker="clickhouse/test-base",
        requires=[
            ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD,
            ArtifactNames.UNITTEST_LLVM_COVERAGE,
            *LLVM_ARTIFACTS_LIST,
        ],
        provides=[
            ArtifactNames.LLVM_COVERAGE_INFO_FILE,
        ],
        command="python3 ./ci/jobs/llvm_coverage_job.py",
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/llvm_coverage_hook.py"],
        digest_config=Job.CacheDigestConfig(
            # llvm_coverage_job.py shells out to all of these; a change to any of
            # them must mark this job (and, transitively via `requires`, the
            # coverage build and every FT/IT/UT coverage shard) as affected -
            # otherwise praktika's changed-files filter drops the job even when
            # filter_job.py's should_skip_job says it should run.
            include_paths=[
                "./ci/jobs/llvm_coverage_job.py",
                "./ci/jobs/scripts/merge_llvm_coverage.sh",
                "./ci/jobs/scripts/generate_diff_coverage_report.sh",
                "./ci/jobs/scripts/print_uncovered_code.py",
                "./ci/jobs/scripts/dedup_lcov_instantiations.py",
                "./ci/jobs/scripts/job_hooks/llvm_coverage_hook.py",
            ],
        ),
        timeout=3600,
        enable_gh_auth=True,
    )
    promql_compliance_job = Job.Config(
        name=JobNames.PROMQL_COMPLIANCE,
        runs_on=RunnerLabels.ARM_TINY,
        run_in_docker="clickhouse/test-base",
        # Wait for integration upload post-hooks, including failed integration jobs.
        run_after=[
            j.name
            for j in (
                integration_test_jobs_required + integration_test_jobs_non_required
            )
        ],
        command="python3 ./ci/jobs/promql_compliance_job.py",
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/promql_compliance_comment_hook.py",
        ],
        # No digest_config: output depends on PR SHA and S3 JSON; script-only cache keys
        # would skip later labeled PRs after one successful run.
        timeout=600,
        enable_gh_auth=True,
        allow_failure=True,
    )

    sign_macos_binary_jobs = Job.Config(
        name=JobNames.SIGN_MACOS,
        runs_on=RunnerLabels.RELEASE_RUNNER,
        command="python3 ./ci/jobs/sign_macos_binary.py --build-type {PARAMETER}",
        run_in_docker="clickhouse/utils+--network=host+root",
        timeout=3600,
        digest_config=Job.CacheDigestConfig(
            include_paths=build_digest_config.include_paths
            + [
                "./ci/jobs/sign_macos_binary.py",
                "./ci/jobs/scripts/sign_macos_binary",
            ],
            with_git_submodules=True,
        ),
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_DARWIN,
            requires=[ArtifactNames.CH_AMD_DARWIN_PLAIN],
            provides=[ArtifactNames.CH_AMD_DARWIN_SIGNED],
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            requires=[ArtifactNames.CH_ARM_DARWIN_PLAIN],
            provides=[ArtifactNames.CH_ARM_DARWIN_SIGNED],
        ),
    )
