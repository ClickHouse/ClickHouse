from praktika import Job
from praktika.utils import Utils

from ci.defs.defs import (
    LLVM_ARTIFACTS_LIST,
    LLVM_FT_NUM_BATCHES,
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
        "./utils/list-licenses",
        "./utils/self-extracting-executable",
    ],
    with_git_submodules=True,
)

fast_test_digest_config = Job.CacheDigestConfig(
    include_paths=[
        "./ci/jobs/fast_test.py",
        "./ci/jobs/scripts/clickhouse_proc.py",
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
    run_in_docker=f"clickhouse/stateless-test+--memory={LIMITED_MEM}+--cgroupns=host+--cap-add=SYS_PTRACE+--privileged+--security-opt seccomp=unconfined+--tmpfs /tmp/clickhouse:mode=1777+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/etc/clickhouse-server1:/etc/clickhouse-server1+--volume=./ci/tmp/etc/clickhouse-server2:/etc/clickhouse-server2+--volume=./ci/tmp/var/log:/var/log+root",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_tests.py",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/functional_tests_results.py",
            "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
            "./ci/praktika/cidb.py",
            "./tests/queries",
            "./tests/clickhouse-test",
            "./tests/config",
            "./tests/*.txt",
            "./ci/docker/stateless-test",
            "./ci/jobs/scripts/functional_tests/setup_minio.sh",
        ],
    ),
    result_name_for_cidb="Tests",
    timeout=int(3600 * 2.5),
)

common_unit_test_job_config = Job.Config(
    name=JobNames.UNITTEST,
    runs_on=[],  # from parametrize()
    command=f"python3 ./ci/jobs/unit_tests_job.py",
    run_in_docker="clickhouse/test-base+--privileged",
    digest_config=Job.CacheDigestConfig(
        include_paths=["./ci/jobs/unit_tests_job.py"],
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
            "./tests/integration/",
            "./ci/docker/integration",
            "./ci/jobs/scripts/docker_in_docker.sh",
        ],
    ),
    run_in_docker=f"clickhouse/integration-tests-runner+root+--memory={LIMITED_MEM}+--privileged+--dns-search='.'+--security-opt seccomp=unconfined+--cap-add=SYS_PTRACE+{docker_sock_mount}+--volume=clickhouse_integration_tests_volume:/var/lib/docker+--cgroupns=host",
    post_hooks=["python3 ci/jobs/scripts/job_hooks/docker_volume_clean_up_hook.py"],
)


class JobConfigs:
    style_check = Job.Config(
        name=JobNames.STYLE_CHECK,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/check_style.py",
        run_in_docker="clickhouse/style-test",
        enable_commit_status=True,
    )
    pr_body = Job.Config(
        name=JobNames.PR_BODY,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/pr_formatter_job.py",
        allow_merge_on_failure=True,
        enable_gh_auth=True,
    )
    code_review = Job.Config(
        name=JobNames.CODE_REVIEW,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/copilot_review_job.py --pre",
    )
    ci_results_review = Job.Config(
        name=JobNames.CI_RESULTS_REVIEW,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/copilot_review_job.py --post",
        allow_merge_on_failure=True,
        enable_gh_auth=True,
    )
    ci_tests = Job.Config(
        name=JobNames.CI_TESTS,
        runs_on=RunnerLabels.ARM_LARGE,
        command="python3 ./ci/jobs/ci_tests_job.py",
        timeout=1200,
        run_in_docker=f"clickhouse/integration-tests-runner+root+--privileged+--dns-search='.'+--security-opt seccomp=unconfined+--cap-add=SYS_PTRACE+{docker_sock_mount}+--volume=clickhouse_integration_tests_volume:/var/lib/docker+--cgroupns=host",
        digest_config=Job.CacheDigestConfig(include_paths=["./ci"]),
        post_hooks=["python3 ci/jobs/scripts/job_hooks/docker_volume_clean_up_hook.py"],
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
        command="python3 ./ci/jobs/fast_test.py --set-status-success",
        digest_config=fast_test_digest_config,
        result_name_for_cidb="Darwin tests",
        allow_merge_on_failure=True,
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/clickhouse_test_cleanup_hook.py",
            "sudo rm -rf /Users/ec2-user/actions-runner/_work/ClickHouse/ClickHouse/ci/tmp/run*",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            runs_on=RunnerLabels.MACOS_ARM_SMALL,
            requires=[ArtifactNames.CH_ARM_DARWIN_BIN],
        ),
    )
    smoke_tests_macos = Job.Config(
        name=JobNames.SMOKE_TEST_MACOS,
        runs_on=RunnerLabels.MACOS_AMD_SMALL,
        command="python3 ./ci/jobs/smoke_test.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/smoke_test.py",
            ],
        ),
        requires=[ArtifactNames.CH_AMD_DARWIN_BIN],
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
            provides=[ArtifactNames.CH_AMD_DEBUG, ArtifactNames.DEB_AMD_DEBUG],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_ASAN_UBSAN,
            provides=[
                ArtifactNames.CH_AMD_ASAN_UBSAN,
                ArtifactNames.DEB_AMD_ASAN_UBSAN,
                ArtifactNames.UNITTEST_AMD_ASAN_UBSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_TSAN,
            provides=[
                ArtifactNames.CH_AMD_TSAN,
                ArtifactNames.DEB_AMD_TSAN,
                ArtifactNames.UNITTEST_AMD_TSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_MSAN,
            provides=[
                ArtifactNames.CH_AMD_MSAN,
                ArtifactNames.DEB_AMD_MSAN,
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
            provides=[ArtifactNames.CH_ARM_DEBUG, ArtifactNames.DEB_ARM_DEBUG],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_ASAN_UBSAN,
            provides=[
                ArtifactNames.CH_ARM_ASAN_UBSAN,
                ArtifactNames.DEB_ARM_ASAN_UBSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_TSAN,
            provides=[
                ArtifactNames.CH_ARM_TSAN,
                ArtifactNames.DEB_ARM_TSAN,
            ],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_MSAN,
            provides=[ArtifactNames.CH_ARM_MSAN, ArtifactNames.DEB_ARM_MSAN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_UBSAN,
            provides=[ArtifactNames.CH_ARM_UBSAN, ArtifactNames.DEB_ARM_UBSAN],
            runs_on=RunnerLabels.ARM_LARGE,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_BINARY,
            provides=[
                ArtifactNames.CH_ARM_BINARY,
                ArtifactNames.PARSER_MEMORY_PROFILER,
            ],
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
            provides=[ArtifactNames.CH_AMD_DARWIN_BIN],
            runs_on=RunnerLabels.AMD_LARGE,  # cannot crosscompile on arm
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            provides=[ArtifactNames.CH_ARM_DARWIN_BIN],
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
            parameter=BuildTypes.ARM_FUZZERS,
            provides=[],
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
            ],
        ),
        timeout=900,
        post_hooks=["python3 ./ci/jobs/scripts/job_hooks/docker_clean_up_hook.py"],
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
            ],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
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
            ],
        ),
        timeout=900,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
                ArtifactNames.CH_AMD_RELEASE,
            ],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
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
    stateless_tests_targeted_pr_jobs = common_ft_job_config.parametrize(
        Job.ParamSet(
            parameter="arm_asan_ubsan, targeted",
            runs_on=RunnerLabels.ARM_LARGE,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
    )
    # --root/--privileged/--cgroupns=host is required for clickhouse-test --memory-limit
    bugfix_validation_ft_pr_job = Job.Config(
        name=JobNames.BUGFIX_VALIDATE_FT,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/functional_tests.py --options BugfixValidation",
        # some tests can be flaky due to very slow disks - use tmpfs for temporary ClickHouse files
        run_in_docker="clickhouse/stateless-test+--network=host+--privileged+--cgroupns=host+root+--security-opt seccomp=unconfined+--tmpfs /tmp/clickhouse:mode=1777",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/functional_tests.py",
                "./tests/queries",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
            ],
        ),
        result_name_for_cidb="Tests",
    )
    lightweight_functional_tests_job = Job.Config(
        name="Quick functional tests",
        command="python3 ./ci/jobs/clickhouse_light.py --path ./ci/tmp/clickhouse",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/clickhouse_light.py",
                "./ci/jobs/queries",
            ],
        ),
        requires=[ArtifactNames.CH_AMD_DEBUG],
        runs_on=RunnerLabels.AMD_SMALL,
    )
    functional_tests_jobs = common_ft_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, distributed plan, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM_CPU,
                requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="amd_asan_ubsan, db disk, distributed plan, sequential",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
            requires=[ArtifactNames.CH_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, old analyzer, s3 storage, DatabaseReplicated, WasmEdge, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_old_s3_db_repl_wasm_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, old analyzer, s3 storage, DatabaseReplicated, WasmEdge, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_old_s3_db_repl_wasm_sequential"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, ParallelReplicas, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, ParallelReplicas, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_sequential"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, AsyncInsert, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_async_parallel"],
        ),
        Job.ParamSet(
            parameter="amd_llvm_coverage, AsyncInsert, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_LLVM_COVERAGE_BUILD],
            provides=[ArtifactNames.LLVM_COVERAGE_FILE + f"_ft_s3_async_sequential"],
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
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_LARGE,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
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
                parameter=f"amd_msan, WasmEdge, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_LARGE,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_msan, WasmEdge, sequential, {batch}/{total_batches}",
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
            for total_batches in (2,)
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
    functional_tests_jobs_azure = common_ft_job_config.set_allow_merge_on_failure(
        True
    ).parametrize(
        Job.ParamSet(
            parameter="arm_asan_ubsan, azure, parallel",
            runs_on=RunnerLabels.ARM_MEDIUM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan, azure, sequential",
            runs_on=RunnerLabels.ARM_SMALL_MEM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
    )
    bugfix_validation_it_job = (
        common_integration_test_job_config.set_name(JobNames.BUGFIX_VALIDATE_IT)
        .set_runs_on(RunnerLabels.AMD_SMALL_MEM)
        .set_command(
            "python3 ./ci/jobs/integration_test_job.py --options BugfixValidation"
        )
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
    )
    stress_test_jobs = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_debug",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_asan_ubsan, s3",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="arm_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_TSAN],
        ),
        Job.ParamSet(
            parameter="arm_msan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_MSAN],
        ),
        Job.ParamSet(
            parameter="arm_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_UBSAN],
        ),
    )
    # might be heavy on azure - run only on master
    stress_test_azure_jobs = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="azure, amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="azure, amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_TSAN],
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
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
    )
    integration_test_asan_flaky_pr_jobs = (
        common_integration_test_job_config.parametrize(
            Job.ParamSet(
                parameter=f"amd_asan_ubsan, flaky",
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
            parameter=f"amd_asan_ubsan, targeted",
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
    ).parametrize(
        Job.ParamSet(
            parameter="amd_release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[ArtifactNames.DEB_AMD_RELEASE],
        ),
        Job.ParamSet(
            parameter="arm_release",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            requires=[ArtifactNames.DEB_ARM_RELEASE],
        ),
    )
    ast_fuzzer_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=[],  # from parametrize()
        command=f"python3 ./ci/jobs/ast_fuzzer_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/jobs/scripts/fuzzer/",
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
    )
    ast_fuzzer_targeted_pr_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/ast_fuzzer_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/find_symbols.py",
                "./ci/jobs/scripts/find_tests.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/jobs/scripts/fuzzer/",
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
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/jobs/scripts/fuzzer/",
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
    stress_test_serverfuzz_jobs = common_stress_job_config.parametrize(
        Job.ParamSet(
            parameter="experimental, serverfuzz, amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, amd_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_release",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_RELEASE],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_debug",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_DEBUG],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_asan_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_asan_ubsan, s3",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_TSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_msan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_MSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.DEB_ARM_UBSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, azure, amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, azure, amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.DEB_AMD_TSAN],
        ),
    )
    buzz_fuzzer_serverfuzz_jobs = Job.Config(
        name=JobNames.BUZZHOUSE,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/buzzhouse_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./ci/jobs/buzzhouse_job.py",
                "./ci/jobs/ast_fuzzer_job.py",
                "./ci/jobs/scripts/log_parser.py",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
                "./ci/jobs/scripts/fuzzer/",
                "./ci/docker/fuzzer",
            ],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="experimental, serverfuzz, amd_debug",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, arm_asan_ubsan",
            runs_on=RunnerLabels.ARM_MEDIUM,
            requires=[ArtifactNames.CH_ARM_ASAN_UBSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, amd_tsan",
            runs_on=RunnerLabels.AMD_MEDIUM,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="experimental, serverfuzz, amd_msan",
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
    clickbench_master_jobs = Job.Config(
        name=JobNames.CLICKBENCH,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="python3 ./ci/jobs/clickbench.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/clickbench.py",
                "./ci/jobs/scripts/clickbench/",
                "./ci/jobs/scripts/functional_tests/setup_log_cluster.sh",
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
    docs_job = Job.Config(
        name=JobNames.DOCS,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "**/*.md",
                "./docs",
                "./ci/jobs/docs_job.py",
                "CHANGELOG.md",
                "./src/Functions",
            ],
        ),
        run_in_docker="clickhouse/docs-builder",
        requires=[ArtifactNames.CH_ARM_BINARY],
        run_after=[JobNames.STYLE_CHECK],
    )
    docs_job_mintlify = Job.Config(
        name=JobNames.DOCS_MINTLIFY,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_job_mintlify.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./docs/docs",
            ],
            exclude_paths=[
                "./docs/en/",
                "./changelogs/"
            ],
        ),
        run_in_docker="clickhouse/docs-builder"
    )
    docker_server = Job.Config(
        name=JobNames.DOCKER_SERVER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/docker_server.py --tag-type head --allow-build-reuse",
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
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/docker_server.py --tag-type head --allow-build-reuse",
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
    sqlancer_master_jobs = Job.Config(
        name=JobNames.SQLANCER,
        runs_on=[],  # from parametrize()
        command="./ci/jobs/sqlancer_job.sh",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/sqlancer_job.sh", "./ci/docker/sqlancer-test"],
        ),
        run_in_docker="clickhouse/sqlancer-test",
        timeout=3600,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
    )
    sqltest_master_job = Job.Config(
        name=JobNames.SQL_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/sqltest_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/sqltest_job.py",
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
                "./tests/sqllogic/",
            ],
        ),
        requires=[ArtifactNames.CH_ARM_RELEASE],
        run_in_docker="clickhouse/stateless-test",
        timeout=10800,
    )
    jepsen_keeper = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/jepsen_check.py keeper",
        requires=["Build (amd_binary)"],
    )
    jepsen_server = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/jepsen_check.py server",
        requires=["Build (amd_binary)"],
    )
    libfuzzer_job = Job.Config(
        name=JobNames.LIBFUZZER_TEST,
        runs_on=RunnerLabels.ARM_MEDIUM,
        command="python3 ./ci/jobs/libfuzzer_test_check.py 'libFuzzer tests'",
        requires=[ArtifactNames.ARM_FUZZERS, ArtifactNames.FUZZERS_CORPUS],
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
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="python3 ./ci/jobs/update_toolchain_dockerfile.py",
        enable_gh_auth=True,
    )
    vector_search_stress_job = Job.Config(
        name="Vector Search Stress",
        runs_on=RunnerLabels.ARM_LARGE,
        run_in_docker="clickhouse/performance-comparison",
        command="python3 ./ci/jobs/vector_search_stress_tests.py",
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
            include_paths=["./ci/jobs/llvm_coverage_job.py"],
        ),
        timeout=3600,
        enable_gh_auth=True,
    )
