from praktika import Job

from ci.defs.defs import ArtifactNames, BuildTypes, JobNames, RunnerLabels

build_digest_config = Job.CacheDigestConfig(
    include_paths=[
        "./src",
        "./contrib/",
        "./CMakeLists.txt",
        "./PreLoad.cmake",
        "./cmake",
        "./base",
        "./programs",
        "./rust",
        "./ci/jobs/build_clickhouse.py",
        "./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        "./utils/list-licenses",
    ],
    with_git_submodules=True,
)

common_ft_job_config = Job.Config(
    name=JobNames.STATELESS,
    runs_on=[],  # from parametrize
    command='python3 ./ci/jobs/functional_tests.py --options "{PARAMETER}"',
    # some tests can be flaky due to very slow disks - use tmpfs for temporary ClickHouse files
    # --cap-add=SYS_PTRACE and --privileged for gdb in docker
    run_in_docker="clickhouse/stateless-test+--cap-add=SYS_PTRACE+--privileged+--security-opt seccomp=unconfined+--tmpfs /tmp/clickhouse+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/etc/clickhouse-server1:/etc/clickhouse-server1+--volume=./ci/tmp/etc/clickhouse-server2:/etc/clickhouse-server2+--volume=./ci/tmp/var/log:/var/log",
    digest_config=Job.CacheDigestConfig(
        include_paths=[
            "./ci/jobs/functional_tests.py",
            "./ci/jobs/scripts/clickhouse_proc.py",
            "./ci/jobs/scripts/functional_tests_results.py",
            "./tests/queries",
            "./tests/clickhouse-test",
            "./tests/config",
            "./tests/*.txt",
            "./ci/docker/stateless-test",
        ],
    ),
    result_name_for_cidb="Tests",
)


class JobConfigs:
    style_check = Job.Config(
        name=JobNames.STYLE_CHECK,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="python3 ./ci/jobs/check_style.py",
        run_in_docker="clickhouse/style-test",
        enable_commit_status=True,
    )
    fast_test = Job.Config(
        name=JobNames.FAST_TEST,
        runs_on=RunnerLabels.BUILDER_AMD,
        command="python3 ./ci/jobs/fast_test.py",
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker="clickhouse/fasttest+--network=host+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/var/log:/var/log",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/fast_test.py",
                "./tests/queries/0_stateless/",
                "./tests/config/",
                "./tests/clickhouse-test",
                "./src",
                "./contrib/",
                "./CMakeLists.txt",
                "./PreLoad.cmake",
                "./cmake",
                "./base",
                "./programs",
                "./rust",
            ],
        ),
        result_name_for_cidb="Tests",
    )
    tidy_build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=[],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        run_in_docker="clickhouse/binary-builder+--network=host",
        timeout=3600 * 4,
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./src",
                "./contrib/",
                "./CMakeLists.txt",
                "./PreLoad.cmake",
                "./cmake",
                "./base",
                "./programs",
                "./rust",
                "./ci/jobs/build_clickhouse.py",
            ],
            with_git_submodules=True,
        ),
    ).parametrize(
        parameter=[
            BuildTypes.AMD_TIDY,
        ],
        provides=[[]],  # [ArtifactNames.CH_TIDY_BIN],
        runs_on=[
            RunnerLabels.BUILDER_AMD,
        ],
    )
    tidy_arm_build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=["Build (amd_tidy)"],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker="clickhouse/binary-builder+--network=host",
        timeout=3600 * 4,
        allow_merge_on_failure=True,
        digest_config=build_digest_config,
    ).parametrize(
        parameter=[
            BuildTypes.ARM_TIDY,
        ],
        runs_on=[
            RunnerLabels.BUILDER_ARM,
        ],
    )
    build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=[],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker="clickhouse/binary-builder+--network=host",
        timeout=3600 * 2,
        digest_config=build_digest_config,
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        parameter=[
            BuildTypes.AMD_DEBUG,
            BuildTypes.AMD_RELEASE,
            BuildTypes.AMD_ASAN,
            BuildTypes.AMD_TSAN,
            BuildTypes.AMD_MSAN,
            BuildTypes.AMD_UBSAN,
            BuildTypes.AMD_BINARY,
            BuildTypes.ARM_RELEASE,
            BuildTypes.ARM_ASAN,
            BuildTypes.ARM_COVERAGE,
            BuildTypes.ARM_BINARY,
        ],
        provides=[
            [
                ArtifactNames.CH_AMD_DEBUG,
                ArtifactNames.DEB_AMD_DEBUG,
            ],
            [
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
            ],
            [
                ArtifactNames.CH_AMD_ASAN,
                ArtifactNames.DEB_AMD_ASAN,
                ArtifactNames.UNITTEST_AMD_ASAN,
            ],
            [
                ArtifactNames.CH_AMD_TSAN,
                ArtifactNames.DEB_AMD_TSAN,
                ArtifactNames.UNITTEST_AMD_TSAN,
            ],
            [
                ArtifactNames.CH_AMD_MSAN,
                ArtifactNames.DEB_AMD_MSAM,
                ArtifactNames.UNITTEST_AMD_MSAN,
            ],
            [
                ArtifactNames.CH_AMD_UBSAN,
                ArtifactNames.DEB_AMD_UBSAN,
                ArtifactNames.UNITTEST_AMD_UBSAN,
            ],
            [
                ArtifactNames.CH_AMD_BINARY,
            ],
            [
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
            ],
            [
                ArtifactNames.CH_ARM_ASAN,
                ArtifactNames.DEB_ARM_ASAN,
            ],
            [ArtifactNames.DEB_COV, ArtifactNames.CH_COV_BIN],
            [ArtifactNames.CH_ARM_BIN],
        ],
        runs_on=[
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_ARM,
            RunnerLabels.BUILDER_ARM,
            RunnerLabels.BUILDER_ARM,
            RunnerLabels.BUILDER_ARM,
        ],
    )
    special_build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=[],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker="clickhouse/binary-builder+--network=host",
        timeout=3600 * 2,
        digest_config=build_digest_config,
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        parameter=[
            BuildTypes.AMD_DARWIN,
            BuildTypes.ARM_DARWIN,
            BuildTypes.ARM_V80COMPAT,
            BuildTypes.AMD_FREEBSD,
            BuildTypes.PPC64LE,
            BuildTypes.AMD_COMPAT,
            BuildTypes.AMD_MUSL,
            BuildTypes.RISCV64,
            BuildTypes.S390X,
            BuildTypes.LOONGARCH64,
            BuildTypes.FUZZERS,
        ],
        provides=[
            [ArtifactNames.CH_AMD_DARWIN_BIN],
            [ArtifactNames.CH_ARM_DARWIN_BIN],
            [ArtifactNames.CH_ARM_V80COMPAT],
            [ArtifactNames.CH_AMD_FREEBSD],
            [ArtifactNames.CH_PPC64LE],
            [ArtifactNames.CH_AMD_COMPAT],
            [ArtifactNames.CH_AMD_MUSL],
            [ArtifactNames.CH_RISCV64],
            [ArtifactNames.CH_S390X],
            [ArtifactNames.CH_LOONGARCH64],
            [],  # no need for fuzzers artifacts in normal pr run [ArtifactNames.FUZZERS, ArtifactNames.FUZZERS_CORPUS],
        ],
        runs_on=[
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_DARWIN,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.ARM_DARWIN,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.ARM_V80COMPAT,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_FREEBSD,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.PPC64LE,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_COMPAT,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_MUSL,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.RISCV64,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.S390X,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.LOONGARCH64
            RunnerLabels.BUILDER_ARM,  # fuzzers
        ],
    )
    builds_for_tests = [b.name for b in build_jobs] + [tidy_build_jobs[0]]
    install_check_jobs = Job.Config(
        name=JobNames.INSTALL_TEST,
        runs_on=[],  # from parametrize()
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/install_check.py"],
        ),
        timeout=900,
    ).parametrize(
        parameter=[
            "release",
            "aarch64",
        ],
        runs_on=[
            RunnerLabels.STYLE_CHECK_AMD,
            RunnerLabels.STYLE_CHECK_ARM,
        ],
        requires=[
            ["Build (amd_release)"],
            ["Build (arm_release)"],
        ],
    )
    stateless_tests_flaky_pr_jobs = common_ft_job_config.parametrize(
        parameter=[
            "amd_asan, flaky check",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            [ArtifactNames.CH_AMD_ASAN],
        ],
    )
    bugfix_validation_ft_pr_job = Job.Config(
        name=JobNames.BUGFIX_VALIDATE_FT,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/functional_tests.py --options BugfixValidation",
        # some tests can be flaky due to very slow disks - use tmpfs for temporary ClickHouse files
        run_in_docker="clickhouse/stateless-test+--network=host+--security-opt seccomp=unconfined+--tmpfs /tmp/clickhouse",
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
    functional_tests_jobs_required = common_ft_job_config.parametrize(
        parameter=[
            "amd_asan, distributed plan, 1/2",
            "amd_asan, distributed plan, 2/2",
            "amd_binary",
            "amd_binary, old analyzer, s3 storage, DatabaseReplicated, 1/2",
            "amd_binary, old analyzer, s3 storage, DatabaseReplicated, 2/2",
            "amd_binary, ParallelReplicas, s3 storage",
            "amd_debug, AsyncInsert, s3 storage",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            [ArtifactNames.CH_AMD_ASAN],
            [ArtifactNames.CH_AMD_ASAN],
            [ArtifactNames.CH_AMD_BINARY],
            [ArtifactNames.CH_AMD_BINARY],
            [ArtifactNames.CH_AMD_BINARY],
            [ArtifactNames.CH_AMD_BINARY],
            [ArtifactNames.CH_AMD_DEBUG],
        ],
    )
    functional_tests_jobs_coverage = common_ft_job_config.set_allow_merge_on_failure(
        True
    ).parametrize(
        parameter=[f"amd_coverage,{i}/6" for i in range(1, 7)],
        runs_on=[RunnerLabels.FUNC_TESTER_ARM for _ in range(6)],
        requires=[[ArtifactNames.CH_COV_BIN] for _ in range(6)],
    )
    functional_tests_jobs_non_required = (
        common_ft_job_config.set_allow_merge_on_failure(True).parametrize(
            parameter=[
                "amd_debug",
                "amd_tsan, 1/3",
                "amd_tsan, 2/3",
                "amd_tsan, 3/3",
                "amd_msan, 1/4",
                "amd_msan, 2/4",
                "amd_msan, 3/4",
                "amd_msan, 4/4",
                "amd_ubsan",
                "amd_debug, distributed plan, s3 storage",
                "amd_tsan, s3 storage, 1/3",
                "amd_tsan, s3 storage, 2/3",
                "amd_tsan, s3 storage, 3/3",
                "arm_binary",
            ],
            runs_on=[
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_AMD,
                RunnerLabels.FUNC_TESTER_ARM,
            ],
            requires=[
                [ArtifactNames.CH_AMD_DEBUG],
                [ArtifactNames.CH_AMD_TSAN],
                [ArtifactNames.CH_AMD_TSAN],
                [ArtifactNames.CH_AMD_TSAN],
                [ArtifactNames.CH_AMD_MSAN],
                [ArtifactNames.CH_AMD_MSAN],
                [ArtifactNames.CH_AMD_MSAN],
                [ArtifactNames.CH_AMD_MSAN],
                [ArtifactNames.CH_AMD_UBSAN],
                [ArtifactNames.CH_AMD_DEBUG],
                [ArtifactNames.CH_AMD_TSAN],
                [ArtifactNames.CH_AMD_TSAN],
                [ArtifactNames.CH_AMD_TSAN],
                [ArtifactNames.CH_ARM_BIN],
            ],
        )
    )
    functional_tests_jobs_azure_master_only = (
        common_ft_job_config.set_allow_merge_on_failure(True).parametrize(
            runs_on=[
                RunnerLabels.FUNC_TESTER_ARM,
                RunnerLabels.FUNC_TESTER_ARM,
                RunnerLabels.FUNC_TESTER_ARM,
            ],
            parameter=[
                "arm_asan, azure, 1/3",
                "arm_asan, azure, 2/3",
                "arm_asan, azure, 3/3",
            ],
            requires=[
                [ArtifactNames.CH_ARM_ASAN],  # azure asan 1
                [ArtifactNames.CH_ARM_ASAN],  # azure asan 2
                [ArtifactNames.CH_ARM_ASAN],  # azure asan 3
            ],
        )
    )
    bugfix_validation_it_job = Job.Config(
        name=JobNames.BUGFIX_VALIDATE_IT,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_debug)"],
    )
    unittest_jobs = Job.Config(
        name=JobNames.UNITTEST,
        runs_on=[],  # from parametrize()
        command=f"python3 ./ci/jobs/unit_tests_job.py",
        run_in_docker="clickhouse/fasttest",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/unit_tests_job.py"],
        ),
    ).parametrize(
        parameter=[
            "asan",
            "tsan",
            "msan",
            "ubsan",
        ],
        runs_on=[
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
            RunnerLabels.BUILDER_AMD,
        ],
        requires=[
            [ArtifactNames.UNITTEST_AMD_ASAN],
            [ArtifactNames.UNITTEST_AMD_TSAN],
            [ArtifactNames.UNITTEST_AMD_MSAN],
            [ArtifactNames.UNITTEST_AMD_UBSAN],
        ],
    )
    stress_test_jobs = Job.Config(
        name=JobNames.STRESS,
        runs_on=[],  # from parametrize()
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/queries/0_stateless/",
                "./tests/ci/stress.py",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./ci/docker/stress-test",
                "./ci/jobs/scripts/clickhouse_proc.py",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "amd_debug",
            "amd_tsan",
            "arm_asan",
            "amd_ubsan",
            "amd_msan",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_ARM,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_debug)"],
            ["Build (amd_tsan)"],
            ["Build (arm_asan)"],
            ["Build (amd_ubsan)"],
            ["Build (amd_msan)"],
        ],
    )
    stress_test_azure_master_jobs = Job.Config(
        name=JobNames.STRESS,
        runs_on=[],  # from parametrize()
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./ci/docker/stress-test",
                "./ci/jobs/scripts/clickhouse_proc.py",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "azure, tsan",
            "azure, msan",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_tsan)"],
            ["Build (amd_msan)"],
        ],
    )
    upgrade_test_jobs = Job.Config(
        name=JobNames.UPGRADE,
        runs_on=["from param"],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/upgrade_check.py",
                "./tests/ci/stress_check.py",
                "./tests/docker_scripts/",
                "./ci/docker/stress-test",
            ]
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "arm_asan",
            "amd_tsan",
            "amd_msan",
            "amd_debug",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_ARM,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (arm_asan)"],
            ["Build (amd_tsan)"],
            ["Build (amd_msan)"],
            ["Build (amd_debug)"],
        ],
    )
    # why it's master only?
    integration_test_asan_master_jobs = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/integration_test_check.py",
                "./tests/ci/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
    ).parametrize(
        parameter=[
            "asan, 1/4",
            "asan, 2/4",
            "asan, 3/4",
            "asan, 4/4",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(4)],
        requires=[["Build (amd_asan)"] for _ in range(4)],
    )
    integration_test_jobs_required = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/integration_test_check.py",
                "./tests/ci/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
    ).parametrize(
        parameter=[
            "asan, old analyzer, 1/6",
            "asan, old analyzer, 2/6",
            "asan, old analyzer, 3/6",
            "asan, old analyzer, 4/6",
            "asan, old analyzer, 5/6",
            "asan, old analyzer, 6/6",
            "release, 1/4",
            "release, 2/4",
            "release, 3/4",
            "release, 4/4",
            "aarch64, distributed plan, 1/4",
            "aarch64, distributed plan, 2/4",
            "aarch64, distributed plan, 3/4",
            "aarch64, distributed plan, 4/4",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(10)]
        + [RunnerLabels.FUNC_TESTER_ARM for _ in range(4)],
        requires=[["Build (amd_asan)"] for _ in range(6)]
        + [["Build (amd_release)"] for _ in range(4)]
        + [["Build (arm_release)"] for _ in range(4)],
    )
    integration_test_jobs_non_required = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/integration_test_check.py",
                "./tests/ci/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "tsan, 1/6",
            "tsan, 2/6",
            "tsan, 3/6",
            "tsan, 4/6",
            "tsan, 5/6",
            "tsan, 6/6",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(6)],
        requires=[["Build (amd_tsan)"] for _ in range(6)],
    )
    integration_test_asan_flaky_pr_job = Job.Config(
        name=JobNames.INTEGRATION + " (asan, flaky check)",
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/integration_test_check.py",
                "./tests/ci/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
        requires=["Build (amd_asan)"],
    )
    compatibility_test_jobs = Job.Config(
        name=JobNames.COMPATIBILITY,
        runs_on=["#from param"],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/compatibility_check.py",
                "./ci/docker/compatibility",
            ],
        ),
    ).parametrize(
        parameter=["release", "aarch64"],
        runs_on=[
            RunnerLabels.STYLE_CHECK_AMD,
            RunnerLabels.STYLE_CHECK_ARM,
        ],
        requires=[["Build (amd_release)"], ["Build (arm_release)"]],
    )
    ast_fuzzer_jobs = Job.Config(
        name=JobNames.ASTFUZZER,
        runs_on=[],  # from parametrize()
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./tests/ci/ci_fuzzer_check.py",
                "./tests/ci/ci_fuzzer_check.py",
                "./ci/jobs/scripts/fuzzer/",
                "./ci/docker/fuzzer",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "amd_debug",
            "arm_asan",
            "amd_tsan",
            "amd_msan",
            "amd_ubsan",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_ARM,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            [ArtifactNames.CH_AMD_DEBUG],
            [ArtifactNames.CH_ARM_ASAN],
            [ArtifactNames.CH_AMD_TSAN],
            [ArtifactNames.CH_AMD_MSAN],
            [ArtifactNames.CH_AMD_UBSAN],
        ],
    )
    buzz_fuzzer_jobs = Job.Config(
        name=JobNames.BUZZHOUSE,
        runs_on=[],  # from parametrize()
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/docker/fuzzer",
                "./tests/ci/ci_fuzzer_check.py",
                "./ci/docker/fuzzer",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "amd_debug",
            "arm_asan",
            "amd_tsan",
            "amd_msan",
            "amd_ubsan",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_ARM,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            [ArtifactNames.CH_AMD_DEBUG],
            [ArtifactNames.CH_ARM_ASAN],
            [ArtifactNames.CH_AMD_TSAN],
            [ArtifactNames.CH_AMD_MSAN],
            [ArtifactNames.CH_AMD_UBSAN],
        ],
    )
    performance_comparison_with_prev_release_jobs = Job.Config(
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
    ).parametrize(
        parameter=[
            "amd_release, prev_release, 1/3",
            "amd_release, prev_release, 2/3",
            "amd_release, prev_release, 3/3",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(3)],
        requires=[[ArtifactNames.CH_AMD_RELEASE] for _ in range(3)],
        provides=[
            [ArtifactNames.PERF_REPORTS_AMD_1_WITH_RELEASE],
            [ArtifactNames.PERF_REPORTS_AMD_2_WITH_RELEASE],
            [ArtifactNames.PERF_REPORTS_AMD_3_WITH_RELEASE],
        ],
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
        parameter=[
            "amd_release, master_head, 1/3",
            "amd_release, master_head, 2/3",
            "amd_release, master_head, 3/3",
            "arm_release, master_head, 1/3",
            "arm_release, master_head, 2/3",
            "arm_release, master_head, 3/3",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(3)]
        + [RunnerLabels.FUNC_TESTER_ARM for _ in range(3)],
        requires=[[ArtifactNames.CH_AMD_RELEASE] for _ in range(3)]
        + [[ArtifactNames.CH_ARM_RELEASE] for _ in range(3)],
        provides=[
            [ArtifactNames.PERF_REPORTS_AMD_1],
            [ArtifactNames.PERF_REPORTS_AMD_2],
            [ArtifactNames.PERF_REPORTS_AMD_3],
            [ArtifactNames.PERF_REPORTS_ARM_1],
            [ArtifactNames.PERF_REPORTS_ARM_2],
            [ArtifactNames.PERF_REPORTS_ARM_3],
        ],
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
        parameter=[
            BuildTypes.AMD_RELEASE,
            BuildTypes.ARM_RELEASE,
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_ARM,
        ],
        requires=[
            [ArtifactNames.CH_AMD_RELEASE],
            [ArtifactNames.CH_ARM_RELEASE],
        ],
    )
    docs_job = Job.Config(
        name=JobNames.Docs,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "**/*.md",
                "./docs",
                "./ci/jobs/docs_job.py",
            ],
        ),
        run_in_docker="clickhouse/docs-builder",
        requires=[JobNames.STYLE_CHECK, ArtifactNames.CH_ARM_BIN],
    )
    docker_sever = Job.Config(
        name=JobNames.DOCKER_SERVER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "tests/ci/docker_server.py",
                "tests/ci/docker_images_helper.py",
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
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "tests/ci/docker_server.py",
                "tests/ci/docker_images_helper.py",
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
        parameter=[
            "amd_debug",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD],
        requires=[
            [ArtifactNames.CH_AMD_DEBUG],
        ],
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
    jepsen_keeper = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_binary)"],
    )
    jepsen_server = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_binary)"],
    )
    libfuzzer_job = Job.Config(
        name=JobNames.LIBFUZZER_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="cd ./tests/ci && python3 libfuzzer_test_check.py 'libFuzzer tests'",
        requires=["Build (fuzzers)"],
    )
