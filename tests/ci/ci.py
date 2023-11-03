import argparse
import json
import os
import concurrent.futures
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Dict, List, Optional
from s3_helper import S3Helper
from digester import DockerDigester, JobDigester
import docker_images_helper
from env_helper import TEMP_PATH

COMMON_STATELESS_TEST_PARAMS = {
    "digest": "STATELESS_TEST",
    "run_command": "functional_test_check.py",
    "timeout": 10800,
}

COMMON_STATEFUL_TEST_PARAMS = {
    "digest": "STATEFUL_TEST",
    "run_command": "functional_test_check.py",
    "timeout": 3600,
}

COMMON_STRESS_TEST_PARAMS = {
    "digest": "STRESS_TEST",
    "run_command": "stress_check.py",
}

COMMON_UPGRADE_TEST_PARAMS = {
    "digest": "UPGRADE_TEST",
    "run_command": "upgrade_check.py",
}

COMMON_AST_FUZZER_TEST_PARAMS = {
    "digest": "AST_FUZZER_TEST",
    "run_command": "ast_fuzzer_check.py",
}

COMMON_INTEGRATION_TEST_PARAM = {
    "digest": "INTEGRATON_TEST",
    "run_command": "integration_test_check.py",
}

COMMON_UNIT_TEST_PARAMS = {
    "digest": "UNIT_TEST",
    "run_command": "unit_tests_check.py",
}

COMMON_PERF_TEST_PARAMS = {
    "digest": "PERF_TEST",
    "run_command": "performance_comparison_check.py",
}

COMMON_SQLLANCER_TEST_PARAMS = {
    "digest": "SQLLANCER_TEST",
    "run_command": "sqlancer_check.py",
}

COMMON_BUILDER_CONFIG = {"digest": {}}  # type: ignore

JOB_CONFIG = {
    # Build jobs must be named by their build_name
    "package_release": {**COMMON_BUILDER_CONFIG},
    "package_debug": {**COMMON_BUILDER_CONFIG},
    "package_aarch64": {**COMMON_BUILDER_CONFIG},
    "binary_release": {**COMMON_BUILDER_CONFIG},
    "package_asan": {**COMMON_BUILDER_CONFIG},
    "package_ubsan": {**COMMON_BUILDER_CONFIG},
    "package_tsan": {**COMMON_BUILDER_CONFIG},
    "package_msan": {**COMMON_BUILDER_CONFIG},
    # special builds
    "binary_aarch64": {**COMMON_BUILDER_CONFIG},
    "binary_tidy": {**COMMON_BUILDER_CONFIG},
    "binary_darwin": {**COMMON_BUILDER_CONFIG},
    "binary_freebsd": {**COMMON_BUILDER_CONFIG},
    "binary_darwin_aarch64": {**COMMON_BUILDER_CONFIG},
    "binary_ppc64le": {**COMMON_BUILDER_CONFIG},
    "binary_amd64_compat": {**COMMON_BUILDER_CONFIG},
    "binary_aarch64_v80compat": {**COMMON_BUILDER_CONFIG},
    "binary_riscv64": {**COMMON_BUILDER_CONFIG},
    "binary_s390x": {**COMMON_BUILDER_CONFIG},
    # DockerServerImages
    "Docker server and keeper images": {
        "digest": {
            "include_paths": ["tests/ci/docker_server.py"],
        },
    },
    # "Style check": {
    #     "digest": {"include_paths": ["."], "exclude_dirs": [".git", "__pycache__"]}
    # },
    "Docs check": {
        "digest": {
            "include_paths": ["tests/ci/docs_check.py"],
            "docker": ["clickhouse/docs-builder"],
        },
        # This field refers to input data for the job. Unlike other jobs that run for "BUILD" this one runs for "DOCS"
        "input": "DOCS",
    },
    "Fast tests": {
        "digest": "FAST_TEST",
    },
    "Compatibility check X86": {
        "digest": "COMPATIBILITY_CHECK",
    },
    "Compatibility check (aarch64)": {
        "digest": "COMPATIBILITY_CHECK",
    },
    # BuildReport
    "ClickHouse build check": {
        "digest": "BUILD_REPORT_CHECK",
    },
    "ClickHouse special build check": {
        "digest": "BUILD_REPORT_CHECK",
    },
    "Install packages (amd64)": {
        "digest": "INSTALL_PACKAGES",
    },
    "Install packages (arm64)": {
        "digest": "INSTALL_PACKAGES",
    },
    # FunctionalStatelessTestRelease
    "Stateless tests (release)": {
        **COMMON_STATELESS_TEST_PARAMS,  # type: ignore[arg-type]
    },
    # FunctionalStatelessTestReleaseDatabaseReplicated
    "Stateless tests (release, DatabaseReplicated)": {
        **COMMON_STATELESS_TEST_PARAMS,  # type: ignore[arg-type]
        "num_batches": 4,
    },
    # FunctionalStatelessTestReleaseWideParts
    "Stateless tests (release, wide parts enabled)": {
        **COMMON_STATELESS_TEST_PARAMS,
    },
    # FunctionalStatelessTestReleaseAnalyzer
    "Stateless tests (release, analyzer)": {
        **COMMON_STATELESS_TEST_PARAMS,
    },
    # FunctionalStatelessTestReleaseS3
    "Stateless tests (release, s3 storage)": {
        **COMMON_STATELESS_TEST_PARAMS,
        "num_batches": 2,
    },
    # FunctionalStatelessTestS3Debug
    "Stateless tests (debug, s3 storage)": {
        **COMMON_STATELESS_TEST_PARAMS,
        "num_batches": 6,
    },
    # FunctionalStatelessTestS3Tsan
    "Stateless tests (tsan, s3 storage)": {
        **COMMON_STATELESS_TEST_PARAMS,
        "num_batches": 5,
    },
    # FunctionalStatelessTestAarch64
    "Stateless tests (aarch64)": {
        **COMMON_STATELESS_TEST_PARAMS,
    },
    # FunctionalStatelessTestAsan
    "Stateless tests (asan)": {
        **COMMON_STATELESS_TEST_PARAMS,
        "num_batches": 4,
    },
    "Stateless tests (tsan)": {
        **COMMON_STATELESS_TEST_PARAMS,
    },
    # FunctionalStatelessTestMsan:
    "Stateless tests (msan)": {
        **COMMON_STATELESS_TEST_PARAMS,
        "num_batches": 6,
    },
    # FunctionalStatelessTestUBsan:
    "Stateless tests (ubsan)": {
        **COMMON_STATELESS_TEST_PARAMS,
        "num_batches": 2,
    },
    # FunctionalStatelessTestDebug:
    "Stateless tests (debug)": {
        **COMMON_STATELESS_TEST_PARAMS,
    },
    # FunctionalStatelessTestFlakyCheck:
    "Stateless tests flaky check (asan)": {
        **COMMON_STATELESS_TEST_PARAMS,
        "timeout": 3600,
    },
    # TestsBugfixCheck:
    "tests bugfix validate check": {
        "digest": {
            # FIXME: add all files for statless and integrstion tests for this job
            "include_paths": [
                "./tests/queries/0_stateless/",
                "./tests/ci/integration_test_check.py",
                "./tests/ci/functional_test_check.py",
                "./tests/ci/bugfix_validate_check.py",
            ],
            "exclude_files": [".md"],
            # FIXME: add all files for statless and integrstion tests for this job
            "docker": [
                "clickhouse/stateless-test",
                "clickhouse/dotnet-client",
                "clickhouse/integration-helper",
                "clickhouse/integration-test",
                "clickhouse/integration-tests-runner",
                "clickhouse/kerberized-hadoop",
                "clickhouse/kerberos-kdc",
                "clickhouse/mysql-golang-client",
                "clickhouse/mysql-java-client",
                "clickhouse/mysql-js-client",
                "clickhouse/mysql-php-client",
                "clickhouse/nginx-dav",
                "clickhouse/postgresql-java-client",
            ],
        },
    },
    # STATEFUL
    #  FunctionalStatefulTestRelease:
    "Stateful tests (release)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestAarch64:
    "Stateful tests (aarch64)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestAsan:
    "Stateful tests (asan)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestTsan:
    "Stateful tests (tsan)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestMsan:
    "Stateful tests (msan)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestUBsan:
    "Stateful tests (ubsan)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestDebug:
    "Stateful tests (debug)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestDebugParallelReplicas:
    "Stateful tests (debug, ParallelReplicas)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestUBsanParallelReplicas:
    "Stateful tests (ubsan, ParallelReplicas)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestMsanParallelReplicas:
    "Stateful tests (msan, ParallelReplicas)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestTsanParallelReplicas:
    "Stateful tests (tsan, ParallelReplicas)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestAsanParallelReplicas:
    "Stateful tests (asan, ParallelReplicas)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # FunctionalStatefulTestReleaseParallelReplicas:
    "Stateful tests (release, ParallelReplicas)": {
        **COMMON_STATEFUL_TEST_PARAMS,
    },
    # STRESS
    # StressTestAsan:
    "Stress test (asan)": {
        **COMMON_STRESS_TEST_PARAMS,
    },
    # StressTestTsan:
    "Stress test (tsan)": {
        **COMMON_STRESS_TEST_PARAMS,
    },
    #  StressTestMsan:
    "Stress test (msan)": {
        **COMMON_STRESS_TEST_PARAMS,
    },
    # StressTestUBsan:
    "Stress test (ubsan)": {
        **COMMON_STRESS_TEST_PARAMS,
    },
    # StressTestDebug:
    "Stress test (debug)": {
        **COMMON_STRESS_TEST_PARAMS,
    },
    # UPGRADE:
    # UpgradeCheckMsan:
    "Upgrade check (msan)": {
        **COMMON_UPGRADE_TEST_PARAMS,
    },
    # UpgradeCheckDebug:
    "Upgrade check (debug)": {
        **COMMON_UPGRADE_TEST_PARAMS,
    },
    # UpgradeCheckAsan:
    "Upgrade check (asan)": {
        **COMMON_UPGRADE_TEST_PARAMS,
    },
    # UpgradeCheckTsan:
    "Upgrade check (tsan)": {
        **COMMON_UPGRADE_TEST_PARAMS,
    },
    # AST FUZZERS:
    # ASTFuzzerTestAsan:
    "AST fuzzer (asan)": {
        **COMMON_AST_FUZZER_TEST_PARAMS,
    },
    # ASTFuzzerTestTsan:
    "AST fuzzer (tsan)": {
        **COMMON_AST_FUZZER_TEST_PARAMS,
    },
    # ASTFuzzerTestUBSan:
    "AST fuzzer (ubsan)": {
        **COMMON_AST_FUZZER_TEST_PARAMS,
    },
    # ASTFuzzerTestMSan:
    "AST fuzzer (msan)": {
        **COMMON_AST_FUZZER_TEST_PARAMS,
    },
    # ASTFuzzerTestDebug:
    "AST fuzzer (debug)": {
        **COMMON_AST_FUZZER_TEST_PARAMS,
    },
    # INTEGRATION
    # IntegrationTestsAsan:
    "Integration tests (asan)": {**COMMON_INTEGRATION_TEST_PARAM, "num_batches": 4},
    # IntegrationTestsAnalyzerAsan:
    "Integration tests (asan, analyzer)": {
        **COMMON_INTEGRATION_TEST_PARAM,
        "num_batches": 6,
    },
    # IntegrationTestsTsan:
    "Integration tests (tsan)": {
        **COMMON_INTEGRATION_TEST_PARAM,
        "num_batches": 6,
    },
    # IntegrationTestsRelease:
    "Integration tests (release)": {
        **COMMON_INTEGRATION_TEST_PARAM,
        "num_batches": 4,
    },
    # IntegrationTestsFlakyCheck:
    "Integration tests flaky check (asan)": {
        **COMMON_INTEGRATION_TEST_PARAM,
    },
    # UNIT TESTS
    # UnitTestsAsan:
    "Unit tests (asan)": {
        **COMMON_UNIT_TEST_PARAMS,
    },
    # UnitTestsReleaseClang:
    "Unit tests (release)": {
        **COMMON_UNIT_TEST_PARAMS,
    },
    # UnitTestsTsan:
    "Unit tests (tsan)": {
        **COMMON_UNIT_TEST_PARAMS,
    },
    # UnitTestsMsan:
    "Unit tests (msan)": {
        **COMMON_UNIT_TEST_PARAMS,
    },
    # UnitTestsUBsan:
    "Unit tests (ubsan)": {
        **COMMON_UNIT_TEST_PARAMS,
    },
    # PERF
    # PerformanceComparisonX86:
    "Performance Comparison": {
        **COMMON_PERF_TEST_PARAMS,
        "num_batches": 4,
    },
    # PerformanceComparisonAarch:
    "Performance Comparison Aarch64": {
        **COMMON_PERF_TEST_PARAMS,
        "num_batches": 4,
    },
    # SQLLANCER
    # SQLancerTestRelease:
    "SQLancer (release)": {
        **COMMON_SQLLANCER_TEST_PARAMS,
    },
    # SQLancerTestDebug:
    "SQLancer (debug)": {
        **COMMON_SQLLANCER_TEST_PARAMS,
    },
    # SQLLogic
    "Sqllogic test (release)": {
        "digest": {
            "include_paths": ["./tests/ci/sqllogic_test.py"],
            "docker": ["clickhouse/sqllogic-test"],
        },
        "run_command": "sqllogic_test.py",
        "timeout": 10800,
    },
    # SQLTest:
    "SQLTest": {
        "digest": {
            "include_paths": ["./tests/ci/sqltest.py"],
            "docker": ["clickhouse/sqltest"],
        },
        "run_command": "sqltest.py",
    },
}

DIGEST_CONFIG = {
    "BUILD": {
        "include_paths": [
            "./src",
            "./contrib/*-cmake",
            "./cmake",
            "./base",
            "./programs",
            "./packages",
        ],
        "exclude_dirs": ["__pycache__"],
        "exclude_files": [".md"],
        "docker": [
            "clickhouse/binary-builder",
        ],
    },
    "DOCS": {"include_paths": ["**/*.md", "./docs"]},
    "BUILDER": {
        "include_paths": [
            "./tests/ci/build_report_check.py",
        ]
    },
    "COMPATIBILITY_CHECK": {
        "include_paths": [
            "./tests/ci/compatibility_check.py",
        ],
        "docker": ["clickhouse/test-old-ubuntu", "clickhouse/test-old-centos"],
    },
    "FAST_TEST": {
        "include_paths": ["./tests/queries/0_stateless/"],
        "exclude_files": [".md"],
        "docker": [
            "clickhouse/fasttest",
        ],
    },
    "STATELESS_TEST": {
        "include_paths": ["./tests/queries/0_stateless/"],
        "exclude_files": [".md"],
        "docker": ["clickhouse/stateless-test"],
    },
    "STATEFUL_TEST": {
        "include_paths": ["./tests/queries/1_stateful/"],
        "exclude_files": [".md"],
        "docker": ["clickhouse/stateful-test"],
    },
    "STRESS_TEST": {
        # FIXME: which tests are stresstest? stateless?
        "include_paths": ["./tests/queries/0_stateless/"],
        "exclude_files": [".md"],
        "docker": ["clickhouse/stress-test"],
    },
    "UPGRADE_TEST": {
        # FIXME: which tests are upgrade? just python?
        "include_paths": ["./tests/ci/upgrade_check.py"],
        "exclude_files": [".md"],
        "docker": ["clickhouse/upgrade-check"],
    },
    "AST_FUZZER_TEST": {
        # FIXME: which tests are AST_FUZZER_TEST? just python?
        "include_paths": ["./tests/ci/ast_fuzzer_check.py"],
        "exclude_files": [".md"],
        "docker": ["clickhouse/fuzzer"],
    },
    "INTEGRATON_TEST": {
        # FIXME: which tests are INTEGRATON_TEST? just python?
        "include_paths": ["./tests/ci/integration_test_check.py"],
        "exclude_files": [".md"],
        "docker": [
            "clickhouse/dotnet-client",
            "clickhouse/integration-helper",
            "clickhouse/integration-test",
            "clickhouse/integration-tests-runner",
            "clickhouse/kerberized-hadoop",
            "clickhouse/kerberos-kdc",
            "clickhouse/mysql-golang-client",
            "clickhouse/mysql-java-client",
            "clickhouse/mysql-js-client",
            "clickhouse/mysql-php-client",
            "clickhouse/nginx-dav",
            "clickhouse/postgresql-java-client",
        ],
    },
    "UNIT_TEST": {
        "include_paths": [
            "./tests/ci/unit_tests_check.py",
        ],
        "docker": ["clickhouse/unit-test"],
    },
    "INSTALL_PACKAGES": {
        "include_paths": [
            "./tests/ci/install_check.py",
        ],
        "docker": ["clickhouse/install-deb-test", "clickhouse/install-rpm-test"],
    },
    "PERF_TEST": {
        "include_paths": [
            "./tests/ci/performance_comparison_check.py",
        ],
        "docker": ["clickhouse/performance-comparison"],
    },
    "SQLLANCER_TEST": {
        "include_paths": [
            "./tests/ci/sqlancer_check.py",
        ],
        "docker": ["clickhouse/sqlancer-test"],
    },
    "BUILD_REPORT_CHECK": {
        "include_paths": [
            "./tests/ci/build_report_check.py",
        ]
    },
}


def is_build_job(job: str) -> bool:
    if "package_" in job or "binary_" in job:
        return True
    return False


def is_test_job(job: str) -> bool:
    return not is_build_job(job) and not "Style" in job


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    parser.add_argument(
        "--configure",
        action="store_true",
        help="Action that configures ci run. Calculates digests, checks job to be executed, generates json output",
    )
    parser.add_argument(
        "--pre",
        action="store_true",
        help="Action that executes prerequesetes for the job provided in --job-name",
    )
    parser.add_argument(
        "--run",
        action="store_true",
        help="Action that executes run action for specified --job-name. run_command must be configured for a given job name.",
    )
    parser.add_argument(
        "--post",
        action="store_true",
        help="Action that executes postrequisites for the job provided in --job-name",
    )
    parser.add_argument(
        "--mark-success",
        action="store_true",
        help="Action that marks job provided in --job-name (and batch privided in --batch) as successfull",
    )
    parser.add_argument(
        "--job-name",
        default="",
        type=str,
        help="Job name as in config",
    )
    parser.add_argument(
        "--batch",
        default=-1,
        type=int,
        help="Current batch number (required for --mark-success), -1 or omit for single-batch job",
    )
    parser.add_argument(
        "--infile",
        default="",
        type=str,
        help="Input json file or json string with ci run config",
    )
    parser.add_argument(
        "--outfile",
        default="",
        type=str,
        required=False,
        help="otput file to write json result to, if not set - stdout",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        default=False,
        help="makes json output pretty formated",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        default=False,
        help="skip fetching docker data from dockerhub for --configure step (for debugging)",
    )
    return parser.parse_args()


def get_file_flag_name(
    job_name: str, digest: str, batch: int = 0, num_batches: int = 1
) -> str:
    if num_batches < 2:
        return f"job_{job_name}_{digest}.ci"
    else:
        return f"job_{job_name}_{digest}_{batch}_{num_batches}.ci"


def get_s3_path(build_digest: str) -> str:
    return f"CI_data/BUILD-{build_digest}/"


def get_s3_path_docs(digest: str) -> str:
    return f"CI_data/DOCS-{digest}/"


def check_missing_images_on_dockerhub(
    image_names: List[str], image_tags: List[str], arch: Optional[str] = None
) -> List[str]:
    """
    Checks missing images on dockerhub.
    Works concurrently for all given images.
    Docker must be logged in.
    """

    def run_docker_command(
        image: str, image_digest: str, arch: Optional[str] = None
    ) -> Dict:
        """
        aux command for fetching single docker manifest
        """
        command = [
            "docker",
            "manifest",
            "inspect",
            f"{image}:{image_digest}" if not arch else f"{image}:{image_digest}-{arch}",
        ]

        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )

        return {
            "image": image,
            "image_digest": image_digest,
            "arch": arch,
            "stdout": process.stdout,
            "stderr": process.stderr,
            "return_code": process.returncode,
        }

    result: List[str] = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [
            executor.submit(run_docker_command, image, tag, arch)
            for image, tag in zip(image_names, image_tags)
        ]

        responses = [
            future.result() for future in concurrent.futures.as_completed(futures)
        ]
        for resp in responses:
            name, stdout, stderr, digest, arch = (
                resp["image"],
                resp["stdout"],
                resp["stderr"],
                resp["image_digest"],
                resp["arch"],
            )
            if stderr:
                if stderr.startswith("no such manifest"):
                    result += (name,)
                else:
                    print(f"Eror: Unknown error: {stderr}, {name}, {arch}")
            elif stdout:
                if "mediaType" in stdout:
                    pass
                else:
                    print(f"Eror: Unknown response: {stdout}")
                    assert False, "FIXME"
            else:
                print(f"Eror: No response for {name}, {digest}, {arch}")
                assert False, "FIXME"
    return result


def main() -> int:
    exit_code = 0
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    args = parse_args(parser)

    if (args.mark_success or args.pre or args.post) and not args.infile:
        print("ERROR: need option --infile to be provided")
        parser.print_help()
        parser.exit(1)
    if args.mark_success or args.pre or args.post or args.run:
        assert args.job_name, "Job name must be provided via --job-name"
        assert (
            args.job_name in JOB_CONFIG
        ), f"Job [{args.job_name}] is not found in the config [{JOB_CONFIG}]"

    indata: Optional[Dict] = None
    if args.infile:
        indata = (
            json.loads(args.infile)
            if not os.path.isfile(args.infile)
            else json.load(open(args.infile))
        )
        assert indata and isinstance(indata, dict), "Invalid --infile json"

    result = {}

    s3 = S3Helper()

    if args.configure:
        docker_data = {}
        if not args.skip_docker:
            # generate docker jobs data
            docker_digester = DockerDigester()
            common_docker_tag = (
                docker_digester.get_total_digest()
            )  # will be used for all multiarch images
            imagename_digest_dict = (
                docker_digester.get_all_digests()
            )  # 'image name - digest' mapping
            images_info = docker_images_helper.get_images_info()
            (
                image_names_amd64,
                image_tags_amd64,
                image_names_aarch64,
                image_tags_aarch64,
            ) = (
                [],
                [],
                [],
                [],
            )

            # a. check missing images
            for name, digest in imagename_digest_dict.items():
                image_names_amd64 += [name]
                image_tags_amd64 += [digest]
                if images_info[name]["only_amd64"]:
                    # FIXME: WA until full arm support
                    continue
                image_names_aarch64 += [name]
                image_tags_aarch64 += [digest]
            print("Start checking missing images in dockerhub")
            missing_aarch64 = check_missing_images_on_dockerhub(
                image_names_aarch64, image_tags_aarch64, "aarch64"
            )
            missing_amd64 = check_missing_images_on_dockerhub(
                image_names_amd64, image_tags_amd64, "amd64"
            )
            missing_multi = check_missing_images_on_dockerhub(
                image_names_amd64, [common_docker_tag] * len(image_names_amd64)
            )
            print("...checking missing images in dockerhub - done")
            docker_data = {
                "tag": common_docker_tag,
                "images": imagename_digest_dict,
                "missing_aarch64": missing_aarch64,
                "missing_amd64": missing_amd64,
                "missing_multi": missing_multi,
            }

        # generate jobs data

        # a. digest each item from the config
        job_digester = JobDigester()
        jobs_params: Dict[str, Dict] = {}
        jobs_to_do: List[str] = []
        jobs_to_skip: List[str] = []
        digests: Dict[str, str] = {}
        build_digest = job_digester.get_job_digest(DIGEST_CONFIG["BUILD"])
        docs_digest = job_digester.get_job_digest(DIGEST_CONFIG["DOCS"])
        print("Calculating job digests - start")
        for job in JOB_CONFIG:
            digest_config = JOB_CONFIG[job]["digest"]
            if isinstance(digest_config, dict):
                # this is config
                digest = job_digester.get_job_digest(digest_config)
            elif isinstance(digest_config, str):
                # this is a reference to config
                digest = job_digester.get_job_digest(DIGEST_CONFIG[digest_config])
            else:
                assert False, "Bug!"
            digests[job] = digest
            print(f"    job [{job}] has digest [{digest}]")
        print("Calculating job digests - done")

        # b. check if we have something done
        path = get_s3_path(build_digest)
        done_files = s3.list_prefix(path)
        done_files = [file.split("/")[-1] for file in done_files]
        print(f"S3 CI files for the build [{build_digest}]: {done_files}")
        docs_path = get_s3_path_docs(docs_digest)
        done_files_docs = s3.list_prefix(docs_path)
        done_files_docs = [file.split("/")[-1] for file in done_files_docs]
        print(f"S3 CI files for the docs [{docs_digest}]: {done_files_docs}")
        done_files += done_files_docs
        for job in digests:
            if job == "BUILD":
                continue
            digest = digests[job]
            num_batches = JOB_CONFIG[job].get("num_batches", 1)
            if num_batches == 1:
                success_flag_name = get_file_flag_name(job, digest)
                if success_flag_name in done_files:
                    jobs_to_skip += (job,)
                else:
                    jobs_to_do.append(job)
                    jobs_params[job] = {"batches": [0], "num_batches": num_batches}
            else:
                batches_to_do: List[int] = []
                for batch in range(num_batches):
                    success_flag_name = get_file_flag_name(
                        job, digest, batch, num_batches
                    )
                    if success_flag_name not in done_files:
                        batches_to_do.append(batch)
                if batches_to_do:
                    jobs_to_do.append(job)
                    jobs_params[job] = {
                        "batches": batches_to_do,
                        "num_batches": num_batches,
                    }
                else:
                    jobs_to_skip += (job,)

        # conclude results
        result = {
            "build": build_digest,
            "docs": docs_digest,
            "jobs_data": {
                "digests": digests,
                "jobs_to_do": jobs_to_do,
                "jobs_to_skip": jobs_to_skip,
                "jobs_params": jobs_params,
            },
            "docker_data": docker_data,
        }

    if args.post:
        if is_build_job(args.job_name):
            # FIXME: avoid using env
            temp_path = Path(TEMP_PATH)
            assert temp_path.is_dir(), f"File [{temp_path}] is not a dir"
            files = list(temp_path.glob(f"*{args.job_name}.json"))
            assert len(files) == 1, f"Which is the report file: {files}?"
            local_report = f"{files[0]}"
            report_name = f"{args.job_name}.json"
            s3_path = get_s3_path(indata["build"]) + report_name
            report_url = s3.upload_file(file_path=local_report, s3_path=s3_path)
            print(
                f"Post action done. Report file [{local_report}] has been uploaded to [{report_url}]"
            )
        else:
            print(f"Post action done. Nothing to do for [{args.job_name}]")
    elif args.run:
        assert (
            "run_command" in JOB_CONFIG[args.job_name]
        ), f"Run command must be configured in JOB_CONFIG for [{args.job_name}] or in GH workflow"
        if "timeout" in JOB_CONFIG[args.job_name]:
            os.environ["KILL_TIMEOUT"] = str(JOB_CONFIG[args.job_name]["timeout"])
        os.environ["CHECK_NAME"] = args.job_name
        process = subprocess.run(shlex.split("env"), check=False)
        run_command = "./tests/ci/" + JOB_CONFIG[args.job_name]["run_command"]
        if ".py" in run_command:
            run_command = "python3 " + run_command
        print(f"Going to start run command [{run_command}]")
        process = subprocess.run(
            shlex.split(run_command),
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True,
            check=False,
        )
        if process.returncode == 0:
            print(f"Run action done for: [{args.job_name}]")
        else:
            print(
                f"Run action failed for: [{args.job_name}] with exit code [{process.returncode}]"
            )
            exit_code = process.returncode
    elif args.pre:
        if is_test_job(args.job_name):
            # FIXME: avoid using env
            temp_path = Path(TEMP_PATH)
            assert temp_path.is_dir(), f"File [{temp_path}] is not a dir"
            path = get_s3_path(indata["build"])
            files = s3.download_files(
                s3_path=path, file_suffix=".json", local_directory=temp_path
            )
            print(
                f"Pre action done. Report files [{files}] have been downloaded from [{path}] to [{temp_path}]"
            )
        else:
            print("Pre action done. Nothing to do for [{args.job_name}]")
    elif args.mark_success:
        job = args.job_name
        num_batches = JOB_CONFIG[job].get("num_batches", 1)
        assert (
            num_batches <= 1 or 0 <= args.batch < num_batches
        ), f"--batch must be provided and in range [0, {num_batches}) for {job}"
        job_input = JOB_CONFIG[job].get("input", "BUILD")
        assert job_input in ("BUILD", "DOCS")
        success_flag_name = get_file_flag_name(
            job, indata["jobs_data"]["digests"][job], args.batch, num_batches
        )
        if job_input == "BUILD":
            path = get_s3_path(indata["build"]) + success_flag_name
        else:
            path = get_s3_path_docs(indata["docs"]) + success_flag_name
        open(success_flag_name, "w").close()
        _ = s3.upload_file(file_path=success_flag_name, s3_path=path)
        os.remove(success_flag_name)
        print(
            f"Job [{job}] with digest [{indata['jobs_data']['digests'][job]}] {f'and batch {args.batch}/{num_batches}' if num_batches > 1 else ''} marked as successful. path: [{path}]"
        )

    if args.outfile:
        with open(args.outfile, "w") as file:
            if isinstance(result, str):
                print(result, file=file)
            elif isinstance(result, dict):
                print(json.dumps(result, indent=2 if args.pretty else None), file=file)
            else:
                raise AssertionError(f"Unexpected type for 'res': {type(result)}")
    else:
        if isinstance(result, str):
            print(result)
        elif isinstance(result, dict):
            print(json.dumps(result, indent=2 if args.pretty else None))
        else:
            raise AssertionError(f"Unexpected type for 'res': {type(result)}")

    return exit_code


if __name__ == "__main__":
    os.chdir(f"{os.path.dirname(__file__)}/../../")
    sys.exit(main())
