from praktika import Job

from ci.defs.defs import ArtifactNames, BuildTypes, JobNames, RunnerLabels


class JobConfigs:
    docker_build_arm = Job.Config(
        name=JobNames.DOCKER_BUILDS_ARM,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./docker",
                "./tests/ci/docker_images_check.py",
            ],
        ),
        command="python3 ./tests/ci/docker_images_check.py --suffix aarch64",
    )
    docker_build_amd = Job.Config(
        name=JobNames.DOCKER_BUILDS_AMD,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./docker",
                "./tests/ci/docker_images_check.py",
            ],
        ),
        command="python3 ./tests/ci/docker_images_check.py --suffix amd64 --multiarch-manifest",
        requires=[JobNames.DOCKER_BUILDS_ARM],
    )
    style_check = Job.Config(
        name=JobNames.STYLE_CHECK,
        runs_on=RunnerLabels.STYLE_CHECK_ARM,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=[JobNames.DOCKER_BUILDS_AMD],
        enable_commit_status=True,
    )
    fast_test = Job.Config(
        name=JobNames.FAST_TEST,
        runs_on=RunnerLabels.BUILDER_AMD,
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/queries/0_stateless/",
                "./tests/docker_scripts/",
                "./tests/config/",
                "./tests/clickhouse-test",
                "./tests/ci/fast_test_check.py",
                "./docker",
                "./src",
                "./contrib/",
                "./CMakeLists.txt",
                "./PreLoad.cmake",
                "./cmake",
                "./base",
                "./programs",
                "./docker/packager/packager",
                "./rust",
            ]
        ),
        requires=[JobNames.DOCKER_BUILDS_AMD],
        timeout=3000,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        provides=[ArtifactNames.FAST_TEST],
    )
    build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=["...from params..."],
        # requires=[JobNames.STYLE_CHECK, JobNames.FAST_TEST],
        command="cd ./tests/ci && eval $(python3 ci_config.py --build-name 'Build ({PARAMETER})' | sed 's/^/export /') && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./src",
                "./contrib/",
                "./CMakeLists.txt",
                "./PreLoad.cmake",
                "./cmake",
                "./base",
                "./programs",
                "./docker/packager/packager",
                "./rust",
                "./tests/ci/build_check.py",
                "./tests/performance",
            ],
            with_git_submodules=True,
        ),
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
            BuildTypes.AMD_TIDY,
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
                ArtifactNames.PERFORMANCE_PACKAGE_AMD,
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
                ArtifactNames.UNITTEST_AMD_BINARY,
            ],
            [
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
                ArtifactNames.PERFORMANCE_PACKAGE_ARM,
            ],
            [
                ArtifactNames.CH_ARM_ASAN,
                ArtifactNames.DEB_ARM_ASAN,
            ],
            [ArtifactNames.CH_TIDY_BIN],
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
            RunnerLabels.BUILDER_AMD,
        ],
    )
    special_build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=["...from params..."],
        command="cd ./tests/ci && eval $(python3 ci_config.py --build-name 'Build ({PARAMETER})' | sed 's/^/export /') && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./src",
                "./contrib/",
                "./CMakeLists.txt",
                "./PreLoad.cmake",
                "./cmake",
                "./base",
                "./programs",
                "./docker/packager/packager",
                "./rust",
                "./tests/ci/build_check.py",
                "./tests/performance",
            ],
            with_git_submodules=True,
        ),
    ).parametrize(
        parameter=[
            BuildTypes.AMD_COVERAGE,
            BuildTypes.ARM_BINARY,
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
            # BuildTypes.FUZZERS,
        ],
        provides=[
            [ArtifactNames.DEB_AMD_COV, ArtifactNames.CH_AMD_COV_BIN],
            [ArtifactNames.CH_ARM_BIN],
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
            # [ArtifactNames.FUZZERS, ArtifactNames.FUZZERS_CORPUS],
        ],
        runs_on=[
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_COVERAGE
            RunnerLabels.BUILDER_ARM,  # BuildTypes.ARM_BINARY
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_DARWIN,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.ARM_DARWIN,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.ARM_V80COMPAT,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_FREEBSD,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.PPC64LE,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_COMPAT,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.AMD_MUSL,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.RISCV64,
            RunnerLabels.BUILDER_AMD,  # BuildTypes.S390X,
            RunnerLabels.BUILDER_ARM,  # BuildTypes.LOONGARCH64
            # RunnerLabels.BUILDER_ARM,  # BuildTypes.FUZZERS
        ],
    )
    install_check_jobs = Job.Config(
        name=JobNames.INSTALL_TEST,
        runs_on=["..."],
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
    stateless_tests_flaky_pr_jobs = Job.Config(
        name=JobNames.STATELESS,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/functional_test_check.py",
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./docker",
            ],
        ),
    ).parametrize(
        parameter=[
            "asan, flaky check",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_asan)"],
        ],
    )
    functional_tests_jobs_required = Job.Config(
        name=JobNames.STATELESS,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/functional_test_check.py",
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./docker",
            ],
        ),
    ).parametrize(
        parameter=[
            "asan, 1/2",
            "asan, 2/2",
            "release",
            "release, old analyzer, s3, DatabaseReplicated, 1/2",
            "release, old analyzer, s3, DatabaseReplicated, 2/2",
            "release, ParallelReplicas, s3 storage",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_asan)"],
            ["Build (amd_asan)"],
            ["Build (amd_release)"],
            ["Build (amd_release)"],
            ["Build (amd_release)"],
            ["Build (amd_release)"],
        ],
    )
    functional_tests_jobs_non_required = Job.Config(
        name=JobNames.STATELESS,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/functional_test_check.py",
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./docker",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "debug",
            "tsan, 1/3",
            "tsan, 2/3",
            "tsan, 3/3",
            "msan, 1/4",
            "msan, 2/4",
            "msan, 3/4",
            "msan, 4/4",
            "ubsan",
            "coverage, 1/6",
            "coverage, 2/6",
            "coverage, 3/6",
            "coverage, 4/6",
            "coverage, 5/6",
            "coverage, 6/6",
            "debug, s3 storage",
            "tsan, s3 storage, 1/3",
            "tsan, s3 storage, 2/3",
            "tsan, s3 storage, 3/3",
            "aarch64",
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
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_ARM,
        ],
        requires=[
            ["Build (amd_debug)"],
            ["Build (amd_tsan)"],
            ["Build (amd_tsan)"],
            ["Build (amd_tsan)"],
            ["Build (amd_msan)"],
            ["Build (amd_msan)"],
            ["Build (amd_msan)"],
            ["Build (amd_msan)"],
            ["Build (amd_ubsan)"],
            ["Build (amd_coverage)"],
            ["Build (amd_coverage)"],
            ["Build (amd_coverage)"],
            ["Build (amd_coverage)"],
            ["Build (amd_coverage)"],
            ["Build (amd_coverage)"],
            ["Build (amd_debug)"],
            ["Build (amd_tsan)"],
            ["Build (amd_tsan)"],
            ["Build (amd_tsan)"],
            ["Build (arm_release)"],
        ],
    )
    functional_tests_jobs_azure_master_only = Job.Config(
        name=JobNames.STATELESS,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/functional_test_check.py",
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./docker",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "azure, asan, 1/3",
            "azure, asan, 2/3",
            "azure, asan, 3/3",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_asan)"],  # azure asan 1
            ["Build (amd_asan)"],  # azure asan 2
            ["Build (amd_asan)"],  # azure asan 3
        ],
    )
    bugfix_validation_job = Job.Config(
        name=JobNames.BUGFIX_VALIDATE,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_debug)"],
    )
    unittest_jobs = Job.Config(
        name=JobNames.UNITTEST,
        runs_on=["..params.."],
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/unit_tests_check.py", "./docker"],
        ),
    ).parametrize(
        parameter=[
            "binary",
            "asan",
            "tsan",
            "msan",
            "ubsan",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_binary)"],
            ["Build (amd_asan)"],
            ["Build (amd_tsan)"],
            ["Build (amd_msan)"],
            ["Build (amd_ubsan)"],
        ],
    )
    stress_test_jobs = Job.Config(
        name=JobNames.STRESS,
        runs_on=["..."],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./docker",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "debug",
            "tsan",
            "asan",
            "ubsan",
            "msan",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_debug)"],
            ["Build (amd_tsan)"],
            ["Build (amd_asan)"],
            ["Build (amd_ubsan)"],
            ["Build (amd_msan)"],
        ],
    )
    stress_test_azure_master_jobs = Job.Config(
        name=JobNames.STRESS,
        runs_on=["..."],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/queries/0_stateless/",
                "./tests/clickhouse-test",
                "./tests/config",
                "./tests/*.txt",
                "./tests/docker_scripts/",
                "./docker",
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
                "./docker",
            ]
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "asan",
            "tsan",
            "msan",
            "debug",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_asan)"],
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
                "./docker",
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
                "./docker",
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
            "aarch64, 1/4",
            "aarch64, 2/4",
            "aarch64, 3/4",
            "aarch64, 4/4",
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
                "./docker",
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
    integration_test_asan_flaky_pr_jobs = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/integration_test_check.py",
                "./tests/ci/integration_tests_runner.py",
                "./tests/integration/",
                "./docker",
            ],
        ),
    ).parametrize(
        parameter=[
            "asan, flaky check",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(1)],
        requires=[["Build (amd_asan)"] for _ in range(1)],
    )
    compatibility_test_jobs = Job.Config(
        name=JobNames.COMPATIBILITY,
        runs_on=["#from param"],
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/compatibility_check.py",
                "./docker/test/compatibility",
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
        runs_on=["..params.."],
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "debug",
            "asan",
            "tsan",
            "msan",
            "ubsan",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(5)],
        requires=[
            ["Build (amd_debug)"],
            ["Build (amd_asan)"],
            ["Build (amd_tsan)"],
            ["Build (amd_msan)"],
            ["Build (amd_ubsan)"],
        ],
    )
    buzz_fuzzer_jobs = Job.Config(
        name=JobNames.BUZZHOUSE,
        runs_on=["..params.."],
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "debug",
            "asan",
            "tsan",
            "msan",
            "ubsan",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD for _ in range(5)],
        requires=[
            ["Build (amd_debug)"],
            ["Build (amd_asan)"],
            ["Build (amd_tsan)"],
            ["Build (amd_msan)"],
            ["Build (amd_ubsan)"],
        ],
    )
    performance_comparison_amd_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["..params.."],
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/performance_comparison_check.py",
                "./tests/performance/",
            ],
        ),
        allow_merge_on_failure=True,
        enable_commit_status=True,
    ).parametrize(
        parameter=[
            "release, 1/3",
            "release, 2/3",
            "release, 3/3",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
            RunnerLabels.FUNC_TESTER_AMD,
        ],
        requires=[
            ["Build (amd_release)"],
            ["Build (amd_release)"],
            ["Build (amd_release)"],
        ],
    )
    performance_comparison_arm_jobs = Job.Config(
        name=JobNames.PERFORMANCE,
        runs_on=["..params.."],
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./tests/ci/performance_comparison_check.py",
                "./tests/performance/",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "aarch64, 1/3",
            "aarch64, 2/3",
            "aarch64, 3/3",
        ],
        runs_on=[
            RunnerLabels.FUNC_TESTER_ARM,
            RunnerLabels.FUNC_TESTER_ARM,
            RunnerLabels.FUNC_TESTER_ARM,
        ],
        requires=[
            ["Build (arm_release)"],
            ["Build (arm_release)"],
            ["Build (arm_release)"],
        ],
    )
    clickbench_master_jobs = Job.Config(
        name=JobNames.CLICKBENCH,
        runs_on=["..params.."],
        command=f"cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/clickbench.py", "./docker"],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        parameter=[
            "release",
            "aarch64",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD, RunnerLabels.FUNC_TESTER_ARM],
        requires=[
            ["Build (amd_release)"],
            ["Build (arm_release)"],
        ],
    )
    docs_job = Job.Config(
        name=JobNames.Docs,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "**/*.md",
                "./docs",
                "tests/ci/docs_check.py",
                "./docker/docs",
            ],
        ),
        requires=[JobNames.STYLE_CHECK],
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
    )
    sqlancer_master_jobs = Job.Config(
        name=JobNames.SQLANCER,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
    ).parametrize(
        parameter=[
            "release",
            "debug",
        ],
        runs_on=[RunnerLabels.FUNC_TESTER_AMD, RunnerLabels.FUNC_TESTER_AMD],
        requires=[
            ["Build (amd_release)"],
            ["Build (amd_debug)"],
        ],
    )
    sqltest_master_job = Job.Config(
        name=JobNames.SQL_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/sqltest.py"],
        ),
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_release)"],
    )
    # TODO: run by labels Labels.PR_BUGFIX, Labels.PR_CRITICAL_BUGFIX
    bugfix_validation = Job.Config(
        name=JobNames.BUGFIX_VALIDATE,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        digest_config=Job.CacheDigestConfig(
            include_paths=["./tests/ci/sqltest.py"],
        ),
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_debug)"],
    )
    # TODO: run by labels Labels.JEPSEN_TEST
    jepsen_keeper = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_binary)"],
    )
    # TODO: run by labels Labels.JEPSEN_TEST
    jepsen_server = Job.Config(
        name=JobNames.JEPSEN_KEEPER,
        runs_on=RunnerLabels.STYLE_CHECK_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (amd_binary)"],
    )
    # TODO: run by label Tags.libFuzzer
    libfuzzer_jjob = Job.Config(
        name=JobNames.LIBFUZZER_TEST,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="cd ./tests/ci && python3 ci.py --run-from-praktika",
        requires=["Build (fuzzers)"],
    )
