from praktika import Job
from praktika.utils import Utils

from ci.defs.defs import ArtifactNames, BuildTypes, JobNames, RunnerLabels

LIMITED_MEM = Utils.physical_memory() - 2 * 1024**3

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
    run_in_docker=f"clickhouse/stateless-test+--memory={LIMITED_MEM}+--cap-add=SYS_PTRACE+--privileged+--security-opt seccomp=unconfined+--tmpfs /tmp/clickhouse+--volume=./ci/tmp/var/lib/clickhouse:/var/lib/clickhouse+--volume=./ci/tmp/etc/clickhouse-client:/etc/clickhouse-client+--volume=./ci/tmp/etc/clickhouse-server:/etc/clickhouse-server+--volume=./ci/tmp/etc/clickhouse-server1:/etc/clickhouse-server1+--volume=./ci/tmp/etc/clickhouse-server2:/etc/clickhouse-server2+--volume=./ci/tmp/var/log:/var/log",
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

BINARY_DOCKER_COMMAND = (
    "clickhouse/binary-builder+--network=host+"
    f"--memory={Utils.physical_memory() * 95 // 100}+"
    f"--memory-reservation={Utils.physical_memory() * 9 // 10}"
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
    tidy_build_arm_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=[],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        run_in_docker=BINARY_DOCKER_COMMAND,
        timeout=3600 * 4,
        digest_config=build_digest_config,
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.ARM_TIDY,
            provides=[],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
    )
    tidy_build_amd_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=[],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        run_in_docker=BINARY_DOCKER_COMMAND,
        timeout=3600 * 4,
        digest_config=build_digest_config,
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_TIDY,
            provides=[],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
    )
    build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=[],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker=BINARY_DOCKER_COMMAND,
        timeout=3600 * 2,
        digest_config=build_digest_config,
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_DEBUG,
            provides=[ArtifactNames.CH_AMD_DEBUG, ArtifactNames.DEB_AMD_DEBUG],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_RELEASE,
            provides=[
                ArtifactNames.CH_AMD_RELEASE,
                ArtifactNames.DEB_AMD_RELEASE,
                ArtifactNames.RPM_AMD_RELEASE,
                ArtifactNames.TGZ_AMD_RELEASE,
            ],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_ASAN,
            provides=[
                ArtifactNames.CH_AMD_ASAN,
                ArtifactNames.DEB_AMD_ASAN,
                ArtifactNames.UNITTEST_AMD_ASAN,
            ],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_TSAN,
            provides=[
                ArtifactNames.CH_AMD_TSAN,
                ArtifactNames.DEB_AMD_TSAN,
                ArtifactNames.UNITTEST_AMD_TSAN,
            ],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_MSAN,
            provides=[
                ArtifactNames.CH_AMD_MSAN,
                ArtifactNames.DEB_AMD_MSAM,
                ArtifactNames.UNITTEST_AMD_MSAN,
            ],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_UBSAN,
            provides=[
                ArtifactNames.CH_AMD_UBSAN,
                ArtifactNames.DEB_AMD_UBSAN,
                ArtifactNames.UNITTEST_AMD_UBSAN,
            ],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_BINARY,
            provides=[ArtifactNames.CH_AMD_BINARY],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_RELEASE,
            provides=[
                ArtifactNames.CH_ARM_RELEASE,
                ArtifactNames.DEB_ARM_RELEASE,
                ArtifactNames.RPM_ARM_RELEASE,
                ArtifactNames.TGZ_ARM_RELEASE,
            ],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_ASAN,
            provides=[
                ArtifactNames.CH_ARM_ASAN,
                ArtifactNames.DEB_ARM_ASAN,
            ],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_COVERAGE,
            provides=[
                ArtifactNames.DEB_COV,
                ArtifactNames.CH_COV_BIN,
            ],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_BINARY,
            provides=[ArtifactNames.CH_ARM_BINARY],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
    )
    special_build_jobs = Job.Config(
        name=JobNames.BUILD,
        runs_on=[],  # from parametrize()
        requires=[],
        command='python3 ./ci/jobs/build_clickhouse.py --build-type "{PARAMETER}"',
        # --network=host required for ec2 metadata http endpoint to work
        run_in_docker=BINARY_DOCKER_COMMAND,
        timeout=3600 * 2,
        digest_config=build_digest_config,
        post_hooks=[
            "python3 ./ci/jobs/scripts/job_hooks/build_master_head_hook.py",
            "python3 ./ci/jobs/scripts/job_hooks/build_profile_hook.py",
        ],
    ).parametrize(
        Job.ParamSet(
            parameter=BuildTypes.AMD_DARWIN,
            provides=[ArtifactNames.CH_AMD_DARWIN_BIN],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_DARWIN,
            provides=[ArtifactNames.CH_ARM_DARWIN_BIN],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.ARM_V80COMPAT,
            provides=[ArtifactNames.CH_ARM_V80COMPAT],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_FREEBSD,
            provides=[ArtifactNames.CH_AMD_FREEBSD],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.PPC64LE,
            provides=[ArtifactNames.CH_PPC64LE],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_COMPAT,
            provides=[ArtifactNames.CH_AMD_COMPAT],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.AMD_MUSL,
            provides=[ArtifactNames.CH_AMD_MUSL],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.RISCV64,
            provides=[ArtifactNames.CH_RISCV64],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.S390X,
            provides=[ArtifactNames.CH_S390X],
            runs_on=RunnerLabels.BUILDER_AMD,
        ),
        Job.ParamSet(
            parameter=BuildTypes.LOONGARCH64,
            provides=[ArtifactNames.CH_LOONGARCH64],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
        Job.ParamSet(
            parameter=BuildTypes.FUZZERS,
            provides=[],
            runs_on=RunnerLabels.BUILDER_ARM,
        ),
    )
    builds_for_tests = [b.name for b in build_jobs] + [tidy_build_arm_jobs[0]]
    install_check_jobs = Job.Config(
        name=JobNames.INSTALL_TEST,
        runs_on=[],  # from parametrize()
        command="python3 ./ci/jobs/install_check.py --no-rpm --no-tgz",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/install_check.py",
                "./ci/docker/install",
            ],
        ),
        timeout=900,
    ).parametrize(
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=[
                ArtifactNames.DEB_AMD_DEBUG,
                ArtifactNames.CH_AMD_DEBUG,
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
            parameter="amd_asan, flaky check",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
            requires=[ArtifactNames.CH_AMD_ASAN],
        ),
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
    functional_tests_jobs = common_ft_job_config.parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan, distributed plan, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_MEDIUM_CPU,
                requires=[ArtifactNames.CH_AMD_ASAN],
            )
            for total_batches in (2,)
            for batch in range(1, total_batches + 1)
        ],
        Job.ParamSet(
            parameter="amd_asan, distributed plan, sequential",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
            requires=[ArtifactNames.CH_AMD_ASAN],
        ),
        Job.ParamSet(
            parameter="amd_binary, old analyzer, s3 storage, DatabaseReplicated, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_BINARY],
        ),
        Job.ParamSet(
            parameter="amd_binary, old analyzer, s3 storage, DatabaseReplicated, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_BINARY],
        ),
        Job.ParamSet(
            parameter="amd_binary, ParallelReplicas, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_BINARY],
        ),
        Job.ParamSet(
            parameter="amd_binary, ParallelReplicas, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_BINARY],
        ),
        Job.ParamSet(
            parameter="amd_debug, AsyncInsert, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM,  # large machine - no boost, why?
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="amd_debug, AsyncInsert, s3 storage, sequential",
            runs_on=RunnerLabels.AMD_SMALL,
            requires=[ArtifactNames.CH_AMD_DEBUG],
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
                parameter=f"amd_msan, parallel, {batch}/{total_batches}",
                runs_on=RunnerLabels.AMD_LARGE,
                requires=[ArtifactNames.CH_AMD_MSAN],
            )
            for total_batches in (2,)
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
            parameter="amd_ubsan, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM_CPU,
            requires=[ArtifactNames.CH_AMD_UBSAN],
        ),
        Job.ParamSet(
            parameter="amd_ubsan, sequential",
            runs_on=RunnerLabels.AMD_SMALL_MEM,
            requires=[ArtifactNames.CH_AMD_UBSAN],
        ),
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
        Job.ParamSet(
            parameter="amd_tsan, s3 storage, parallel",
            runs_on=RunnerLabels.AMD_MEDIUM_CPU,
            requires=[ArtifactNames.CH_AMD_TSAN],
        ),
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
    functional_tests_jobs_coverage = common_ft_job_config.set_allow_merge_on_failure(
        True
    ).parametrize(
        Job.ParamSet(
            parameter=f"arm_coverage, parallel",
            runs_on=RunnerLabels.ARM_MEDIUM,
            requires=[ArtifactNames.CH_COV_BIN],
        ),
        Job.ParamSet(
            parameter=f"arm_coverage, sequential",
            runs_on=RunnerLabels.ARM_SMALL_MEM,
            requires=[ArtifactNames.CH_COV_BIN],
        ),
    )
    functional_tests_jobs_azure_master_only = (
        common_ft_job_config.set_allow_merge_on_failure(True).parametrize(
            Job.ParamSet(
                parameter="arm_asan, azure, parallel",
                runs_on=RunnerLabels.ARM_MEDIUM,
                requires=[ArtifactNames.CH_ARM_ASAN],
            ),
            Job.ParamSet(
                parameter="arm_asan, azure, sequential",
                runs_on=RunnerLabels.ARM_SMALL_MEM,
                requires=[ArtifactNames.CH_ARM_ASAN],
            ),
        )
    )
    bugfix_validation_it_job = Job.Config(
        name=JobNames.BUGFIX_VALIDATE_IT,
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="python3 ./ci/jobs/integration_test_check.py --validate-bugfix",
    )
    unittest_jobs = Job.Config(
        name=JobNames.UNITTEST,
        runs_on=[],  # from parametrize()
        command=f"python3 ./ci/jobs/unit_tests_job.py",
        run_in_docker="clickhouse/fasttest+--privileged",
        digest_config=Job.CacheDigestConfig(
            include_paths=["./ci/jobs/unit_tests_job.py"],
        ),
    ).parametrize(
        Job.ParamSet(
            parameter="asan",
            runs_on=RunnerLabels.BUILDER_AMD,
            requires=[ArtifactNames.UNITTEST_AMD_ASAN],
        ),
        Job.ParamSet(
            parameter="tsan",
            runs_on=RunnerLabels.BUILDER_AMD,
            requires=[ArtifactNames.UNITTEST_AMD_TSAN],
        ),
        Job.ParamSet(
            parameter="msan",
            runs_on=RunnerLabels.BUILDER_AMD,
            requires=[ArtifactNames.UNITTEST_AMD_MSAN],
        ),
        Job.ParamSet(
            parameter="ubsan",
            runs_on=RunnerLabels.BUILDER_AMD,
            requires=[ArtifactNames.UNITTEST_AMD_UBSAN],
        ),
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
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_debug)"],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_tsan)"],
        ),
        Job.ParamSet(
            parameter="arm_asan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=["Build (arm_asan)"],
        ),
        Job.ParamSet(
            parameter="amd_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_ubsan)"],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_msan)"],
        ),
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
        Job.ParamSet(
            parameter="azure, tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_tsan)"],
        ),
        Job.ParamSet(
            parameter="azure, msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_msan)"],
        ),
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
        Job.ParamSet(
            parameter="amd_asan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_asan)"],
        ),
        Job.ParamSet(
            parameter="amd_tsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_tsan)"],
        ),
        Job.ParamSet(
            parameter="amd_msan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_msan)"],
        ),
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=["Build (amd_debug)"],
        ),
    )
    # why it's master only?
    integration_test_asan_master_jobs = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="python3 ./ci/jobs/integration_test_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/integration_test_check.py",
                "./ci/jobs/scripts/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
    ).parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_AMD,
                requires=[ArtifactNames.CH_AMD_ASAN],
            )
            for total_batches in (4,)
            for batch in range(1, total_batches + 1)
        ]
    )
    integration_test_jobs_required = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="python3 ./ci/jobs/integration_test_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/integration_test_check.py",
                "./ci/jobs/scripts/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
    ).parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_asan, old analyzer, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_AMD,
                requires=[ArtifactNames.CH_AMD_ASAN],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"amd_binary, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_AMD,
                requires=[ArtifactNames.CH_AMD_BINARY],
            )
            for total_batches in (5,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"arm_binary, distributed plan, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_ARM,
                requires=[ArtifactNames.CH_ARM_BINARY],
            )
            for total_batches in (4,)
            for batch in range(1, total_batches + 1)
        ],
    )
    integration_test_jobs_non_required = Job.Config(
        name=JobNames.INTEGRATION,
        runs_on=["from PARAM"],
        command="python3 ./ci/jobs/integration_test_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/integration_test_check.py",
                "./ci/jobs/scripts/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
        allow_merge_on_failure=True,
    ).parametrize(
        *[
            Job.ParamSet(
                parameter=f"amd_tsan, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_AMD,
                requires=[ArtifactNames.CH_AMD_TSAN],
            )
            for total_batches in (6,)
            for batch in range(1, total_batches + 1)
        ]
    )
    integration_test_asan_flaky_pr_job = Job.Config(
        name=JobNames.INTEGRATION + " (amd_asan, flaky check)",
        runs_on=RunnerLabels.FUNC_TESTER_AMD,
        command="python3 ./ci/jobs/integration_test_check.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "./ci/jobs/integration_test_check.py",
                "./ci/jobs/scripts/integration_tests_runner.py",
                "./tests/integration/",
                "./ci/docker/integration",
            ],
        ),
        requires=[ArtifactNames.CH_AMD_ASAN],
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
        Job.ParamSet(
            parameter="release",
            runs_on=RunnerLabels.STYLE_CHECK_AMD,
            requires=["Build (amd_release)"],
        ),
        Job.ParamSet(
            parameter="aarch64",
            runs_on=RunnerLabels.STYLE_CHECK_ARM,
            requires=["Build (arm_release)"],
        ),
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
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN],
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
            parameter="amd_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_UBSAN],
        ),
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
        Job.ParamSet(
            parameter="amd_debug",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_DEBUG],
        ),
        Job.ParamSet(
            parameter="arm_asan",
            runs_on=RunnerLabels.FUNC_TESTER_ARM,
            requires=[ArtifactNames.CH_ARM_ASAN],
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
            parameter="amd_ubsan",
            runs_on=RunnerLabels.FUNC_TESTER_AMD,
            requires=[ArtifactNames.CH_AMD_UBSAN],
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
            for total_batches in (3,)
            for batch in range(1, total_batches + 1)
        ],
        *[
            Job.ParamSet(
                parameter=f"arm_release, master_head, {batch}/{total_batches}",
                runs_on=RunnerLabels.FUNC_TESTER_ARM,
                requires=[ArtifactNames.CH_ARM_RELEASE],
            )
            for total_batches in (3,)
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
            for total_batches in (3,)
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
        name=JobNames.Docs,
        runs_on=RunnerLabels.FUNC_TESTER_ARM,
        command="python3 ./ci/jobs/docs_job.py",
        digest_config=Job.CacheDigestConfig(
            include_paths=[
                "**/*.md",
                "./docs",
                "./ci/jobs/docs_job.py",
                "CHANGELOG.md",
            ],
        ),
        run_in_docker="clickhouse/docs-builder",
        requires=[JobNames.STYLE_CHECK, ArtifactNames.CH_ARM_BINARY],
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
