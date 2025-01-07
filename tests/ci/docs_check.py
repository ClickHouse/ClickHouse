#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from pathlib import Path

from docker_images_helper import get_docker_image, pull_image
from env_helper import REPO_COPY, TEMP_PATH
from pr_info import PRInfo
from report import FAIL, FAILURE, OK, SUCCESS, JobReport, TestResult, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to check the docs integrity",
    )
    parser.add_argument(
        "--docs-branch",
        default="",
        help="a branch to get from docs repository",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="check the docs even if there no changes",
    )
    parser.add_argument("--pull-image", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-pull-image",
        dest="pull_image",
        action="store_false",
        default=argparse.SUPPRESS,
        help="do not pull docker image, use existing",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    pr_info = PRInfo(need_changed_files=True)

    if not pr_info.has_changes_in_documentation() and not args.force:
        logging.info("No changes in documentation")
        JobReport(
            description="No changes in docs",
            test_results=[],
            status=SUCCESS,
            start_time=stopwatch.start_time_str,
            duration=stopwatch.duration_seconds,
            additional_files=[],
        ).dump()
        sys.exit(0)

    if pr_info.has_changes_in_documentation():
        logging.info("Has changes in docs")
    elif args.force:
        logging.info("Check the docs because of force flag")

    docker_image = get_docker_image("clickhouse/docs-builder")
    if args.pull_image:
        docker_image = pull_image(docker_image)

    test_output = temp_path / "docs_check"
    test_output.mkdir(parents=True, exist_ok=True)

    cmd = (
        f"docker run --cap-add=SYS_PTRACE --memory=12g --user={os.geteuid()}:{os.getegid()} "
        f"-e GIT_DOCS_BRANCH={args.docs_branch} -e NODE_OPTIONS='--max-old-space-size=6144' "
        f"--volume={repo_path}:/ClickHouse --volume={test_output}:/output_path "
        f"{docker_image}"
    )

    run_log_path = test_output / "run.log"
    logging.info("Running command: '%s'", cmd)

    test_results = []  # type: TestResults

    test_sw = Stopwatch()
    with TeePopen(f"{cmd} --out-dir /output_path/build", run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
            job_status = SUCCESS
            build_status = OK
            description = "Docs check passed"
        else:
            description = "Docs check failed (non zero exit code)"
            job_status = FAILURE
            build_status = FAIL
            logging.info("Run failed")

    if build_status == OK:
        with open(run_log_path, "r", encoding="utf-8") as lfd:
            for line in lfd:
                if "ERROR" in line:
                    build_status = FAIL
                    job_status = FAILURE
                    break

    test_results.append(
        TestResult("Docs build", build_status, test_sw.duration_seconds, [run_log_path])
    )

    htmltest_log = test_output / "htmltest.log"

    htmltest_existing_errors = {
        "hash does not exist --- en/sql-reference/functions/array-functions.html --> #arrayenumerateuniqarr",
        "hash does not exist --- en/sql-reference/functions/ip-address-functions.html --> ##IPv4NumToString(num)",
        "hash does not exist --- en/sql-reference/functions/other-functions.html --> #filesystemfree",
        "hash does not exist --- en/sql-reference/functions/type-conversion-functions.html --> #toint8orNull",
        "hash does not exist --- en/sql-reference/functions/type-conversion-functions.html --> #touint8orNull",
        "hash does not exist --- en/sql-reference/operators/in.html --> #settings-parallel_replicas_custom_key",
        "hash does not exist --- en/sql-reference/formats.html --> #prettycompactnoescapes",
        "hash does not exist --- en/sql-reference/formats.html --> #data-format-parquet-metadata",
        "hash does not exist --- en/integrations/clickpipes/kafka.html --> #Batching",
        "hash does not exist --- en/integrations/clickpipes/kinesis.html --> #Batching",
        "hash does not exist --- en/integrations/s3/performance.html --> #utilizing-clusters",
        "hash does not exist --- en/integrations/powerbi.html --> #install-clickhouse-connector",
        "hash does not exist --- en/integrations/kafka/clickhouse-kafka-connect-sink.html --> #Troubleshooting",
        "hash does not exist --- en/getting-started/example-datasets/github.html --> #downloading-and-inserting-the-data",
        "hash does not exist --- en/getting-started/example-datasets/github.html --> #queries",
        # pylint: disable=line-too-long
        "hash does not exist --- en/getting-started/example-datasets/github.html --> #U0VMRUNUCiAgICBhdXRob3IsCiAgICBjb3VudElmKGxpbmVfdHlwZSA9ICdDb2RlJykgQVMgY29kZV9saW5lcywKICAgIGNvdW50SWYoKGxpbmVfdHlwZSA9ICdDb21tZW50JykgT1IgKGxpbmVfdHlwZSA9ICdQdW5jdCcpKSBBUyBjb21tZW50cywKICAgIGNvZGVfbGluZXMgLyAoY29tbWVudHMgKyBjb2RlX2xpbmVzKSBBUyByYXRpb19jb2RlLAogICAgdG9TdGFydE9mV2Vlayh0aW1lKSBBUyB3ZWVrCkZST00gZ2l0X2NsaWNraG91c2UubGluZV9jaGFuZ2VzCkdST1VQIEJZCiAgICB0aW1lLAogICAgYXV0aG9yCk9SREVSIEJZCiAgICBhdXRob3IgQVNDLAogICAgdGltZSBBU0MKTElNSVQgMTA=",
        "hash does not exist --- en/getting-started/example-datasets/github.html --> #list-files-that-were-rewritten-most-number-of-time-or-by-most-of-authors",
        "hash does not exist --- en/operations/system-tables/trace_log.html --> #system_tables-query_log",
        "hash does not exist --- en/operations/system-tables/query_thread_log.html --> #system_tables-events",
        "hash does not exist --- en/operations/storing-data.html --> #configuring-hdfs",
        "hash does not exist --- en/interfaces/formats.html --> #prettycompactnoescapes",
        "hash does not exist --- en/interfaces/formats.html --> #data-format-parquet-metadata",
        "hash does not exist --- en/optimize/partitioning-key.html --> #ingest-data-in-bulk",
        "hash does not exist --- en/engines/table-engines/mergetree-family/mergetree.html --> #table_engine-mergetree-azure-blob-storage",
        "hash does not exist --- en/engines/table-engines/mergetree-family/mergetree.html --> #hdfs-storage",
        "hash does not exist --- en/engines/table-engines/mergetree-family/mergetree.html --> #web-storage",
        "hash does not exist --- en/engines/table-engines/mergetree-family/mergetree.html --> #dynamic-storage",
        "hash does not exist --- en/manage/security/gcp-private-service-connect.html --> #obtain-gcp-service-attachment-for-private-service-connect",
        "hash does not exist --- en/cloud/bestpractices/low-cardinality-partitioning-key.html --> #ingest-data-in-bulk",
        "hash does not exist --- en/cloud/security/cloud-authentication.html --> #establish-strong-passwords",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantile",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantiledeterministic",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantileexact",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantileexactweighted",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantiletiming",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantiletimingweighted",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantiletdigest",
        "hash does not exist --- zh/sql-reference/aggregate-functions/reference/median.html --> #quantiletdigestweighted",
        "hash does not exist --- zh/sql-reference/functions/string-search-functions.html --> #positionCaseInsensitive",
        "hash does not exist --- zh/sql-reference/functions/string-search-functions.html --> #positionUTF8",
        "hash does not exist --- zh/sql-reference/functions/string-search-functions.html --> #positionCaseInsensitiveUTF8",
        "hash does not exist --- zh/sql-reference/functions/string-search-functions.html --> #multiSearchAllPositions",
        "hash does not exist --- zh/sql-reference/functions/string-search-functions.html --> #extractallgroups-vertical",
        "hash does not exist --- zh/sql-reference/functions/string-search-functions.html --> #hasSubsequence",
        "hash does not exist --- zh/sql-reference/functions/string-search-functions.html --> #hasSubsequenceUTF8",
        "hash does not exist --- zh/sql-reference/table-functions/s3.html --> #wildcards-in-path",
        "hash does not exist --- zh/operations/system-tables/trace_log.html --> #system_tables-query_log",
        "hash does not exist --- zh/operations/system-tables/query_thread_log.html --> #system_tables-events",
        "hash does not exist --- zh/operations/storing-data.html --> #configuring-hdfs",
        "hash does not exist --- zh/operations/settings/permissions-for-queries.html --> #permissions_for_queries",
        "hash does not exist --- zh/interfaces/formats.html --> #protobufsingle",
        "hash does not exist --- zh/interfaces/formats.html --> #data-format-arrow-stream",
        "hash does not exist --- zh/interfaces/formats.html --> #data-format-regexp",
        "hash does not exist --- zh/interfaces/http.html --> #settings-http_zlib_compression_level",
        "hash does not exist --- zh/engines/table-engines/mergetree-family/custom-partitioning-key.html --> #alter_manipulations-with-partitions",
        "hash does not exist --- zh/engines/table-engines/mergetree-family/custom-partitioning-key.html --> #alter_attach-partition",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletdigest.html --> #quantile",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletdigest.html --> #quantiletiming",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletdigest.html --> #quantiles",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/uniqexact.html --> #agg_function-uniq",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletdigestweighted.html --> #quantile",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletdigestweighted.html --> #quantiletiming",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletdigestweighted.html --> #quantiles",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantileexact.html --> #quantiles",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletiming.html --> #quantiles",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletiming.html --> #quantile",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantileexactweighted.html --> #quantileexact",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantileexactweighted.html --> #quantiles",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/uniqcombined.html --> #agg_function-uniqcombined64",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/uniqcombined.html --> #agg_function-uniq",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletimingweighted.html --> #quantiles",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantiletimingweighted.html --> #quantile",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantile.html --> #quantileexact",
        "hash does not exist --- ru/sql-reference/aggregate-functions/reference/quantile.html --> #quantiles",
        "hash does not exist --- ru/sql-reference/functions/geo/geohash.html --> #geohash",
        "hash does not exist --- ru/sql-reference/functions/geo/s2.html --> #geotools2",
        "hash does not exist --- ru/sql-reference/functions/geo/s2.html --> #s2index",
        "hash does not exist --- ru/sql-reference/functions/geo/h3.html --> #h3index",
        "hash does not exist --- ru/sql-reference/functions/string-search-functions.html --> #hasSubsequenceUTF8",
        "hash does not exist --- ru/sql-reference/functions/arithmetic-functions.html --> #multiply",
        "hash does not exist --- ru/sql-reference/functions/arithmetic-functions.html --> #divide",
        "hash does not exist --- ru/sql-reference/statements/alter/column.html --> #alter-how-to-specify-part-expr",
        "hash does not exist --- ru/operations/system-tables/detached_parts.html --> #system_tables-parts",
        "hash does not exist --- ru/operations/system-tables/metric_log.html --> #system_tables-asynchronous_metrics",
        "hash does not exist --- ru/operations/system-tables/metric_log.html --> #system_tables-events",
        "hash does not exist --- ru/operations/system-tables/metric_log.html --> #system_tables-metrics",
        "hash does not exist --- ru/operations/system-tables/asynchronous_metric_log.html --> #system_tables-asynchronous_metrics",
        "hash does not exist --- ru/operations/system-tables/asynchronous_metric_log.html --> #system_tables-metric_log",
        "hash does not exist --- ru/operations/system-tables/events.html --> #system_tables-asynchronous_metrics",
        "hash does not exist --- ru/operations/system-tables/events.html --> #system_tables-metrics",
        "hash does not exist --- ru/operations/system-tables/events.html --> #system_tables-metric_log",
        "hash does not exist --- ru/operations/system-tables/asynchronous_metrics.html --> #system_tables-metrics",
        "hash does not exist --- ru/operations/system-tables/asynchronous_metrics.html --> #system_tables-events",
        "hash does not exist --- ru/operations/system-tables/asynchronous_metrics.html --> #system_tables-metric_log",
        "hash does not exist --- ru/operations/system-tables/metrics.html --> #system_tables-asynchronous_metrics",
        "hash does not exist --- ru/operations/system-tables/metrics.html --> #system_tables-events",
        "hash does not exist --- ru/operations/system-tables/metrics.html --> #system_tables-metric_log",
        "hash does not exist --- ru/operations/system-tables/stack_trace.html --> #system_tables-query_log",
        "hash does not exist --- ru/operations/system-tables/query_thread_log.html --> #system_tables-events",
        "hash does not exist --- ru/operations/system-tables/query_log.html --> #system-tables-introduction",
        "hash does not exist --- ru/operations/system-tables/query_log.html --> #system_tables-events",
        "hash does not exist --- ru/operations/settings/settings.html --> #glob_expansion_max_elements",
        "hash does not exist --- ru/operations/settings/permissions-for-queries.html --> #permissions_for_queries",
        "hash does not exist --- en/sql-reference/statements/create/view.html --> #window-view-experimental",
        "hash does not exist --- en/integrations/javascript.html --> #fn-1",
        "hash does not exist --- en/integrations/javascript.html --> #fn-2",
        "hash does not exist --- en/integrations/javascript.html --> #fn-3",
        "hash does not exist --- en/operations/server-configuration-parameters/settings.html --> #max_concurrent_queries_for_all_users",
        "hash does not exist --- en/operations/server-configuration-parameters/settings.html --> #max_concurrent_queries_for_user",
        "hash does not exist --- en/operations/server-configuration-parameters/settings.html --> #max-table-size-to-drop",
        "hash does not exist --- en/operations/server-configuration-parameters/settings.html --> #max_temporary_data_on_disk_for_user",
        "hash does not exist --- en/operations/server-configuration-parameters/settings.html --> #max_temporary_data_on_disk_size_for_query",
        "hash does not exist --- en/operations/server-configuration-parameters/settings.html --> #server-settings_zookeeper",
        "hash does not exist --- en/operations/server-configuration-parameters/settings.html --> #total-memory-profiler-step",
    }

    test_sw.reset()
    with TeePopen(
        f"{cmd} htmltest -c /ClickHouse/docs/.htmltest.yml /output_path/build",
        htmltest_log,
    ) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
            test_results.append(
                TestResult("htmltest", OK, test_sw.duration_seconds, [htmltest_log])
            )
        else:
            # FIXME: after all errors reported by htmltest are fixed, remove this code
            # and check only exit code
            new_htmltest_errors = False

            with open(htmltest_log, "r", encoding="utf-8") as f:
                lines = f.readlines()
                for line in lines:
                    if "hash does not exist" in line:
                        # pylint: disable=invalid-character-esc
                        # [31m  hash does not exist... => hash does not exist...
                        error = line.split(" ", 1)[1].strip()
                        if error in htmltest_existing_errors:
                            continue

                        new_htmltest_errors = True
                        logging.info("new error: %s", error)

            if new_htmltest_errors:
                logging.info("New htmltest errors found")
                test_results.append(
                    TestResult(
                        "htmltest", FAIL, test_sw.duration_seconds, [htmltest_log]
                    )
                )

                description = "Docs check failed (new 'hash does not exist' errors)"
                build_status = FAIL
                job_status = FAILURE

            else:
                test_results.append(
                    TestResult("htmltest", OK, test_sw.duration_seconds, [htmltest_log])
                )

    JobReport(
        description=description,
        test_results=test_results,
        status=job_status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[],
    ).dump()

    if job_status == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
