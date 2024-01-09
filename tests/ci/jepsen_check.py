#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import time

from pathlib import Path
from typing import Any, List

import boto3  # type: ignore
import requests  # type: ignore
from github import Github

from build_download_helper import get_build_name_for_check
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import RerunHelper, get_commit, post_commit_status
from compress_files import compress_fast
from env_helper import REPO_COPY, TEMP_PATH, S3_BUILDS_BUCKET, S3_DOWNLOAD
from get_robot_token import get_best_robot_token, get_parameter_from_ssm
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from ssh import SSHKey
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results
from version_helper import get_version_from_repo
from build_check import get_release_or_pr

JEPSEN_GROUP_NAME = "jepsen_group"

KEEPER_DESIRED_INSTANCE_COUNT = 3
SERVER_DESIRED_INSTANCE_COUNT = 4

KEEPER_IMAGE_NAME = "clickhouse/keeper-jepsen-test"
KEEPER_CHECK_NAME = "ClickHouse Keeper Jepsen"

SERVER_IMAGE_NAME = "clickhouse/server-jepsen-test"
SERVER_CHECK_NAME = "ClickHouse Server Jepsen"


SUCCESSFUL_TESTS_ANCHOR = "# Successful tests"
INTERMINATE_TESTS_ANCHOR = "# Indeterminate tests"
CRASHED_TESTS_ANCHOR = "# Crashed tests"
FAILED_TESTS_ANCHOR = "# Failed tests"


def _parse_jepsen_output(path: Path) -> TestResults:
    test_results = []  # type: TestResults
    current_type = ""
    with open(path, "r") as f:
        for line in f:
            if SUCCESSFUL_TESTS_ANCHOR in line:
                current_type = "OK"
            elif INTERMINATE_TESTS_ANCHOR in line or CRASHED_TESTS_ANCHOR in line:
                current_type = "ERROR"
            elif FAILED_TESTS_ANCHOR in line:
                current_type = "FAIL"

            if (
                line.startswith("store/clickhouse") or line.startswith("clickhouse")
            ) and current_type:
                test_results.append(TestResult(line.strip(), current_type))

    return test_results


def get_autoscaling_group_instances_ids(asg_client, group_name):
    group_description = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[group_name]
    )
    our_group = group_description["AutoScalingGroups"][0]
    instance_ids = []
    for instance in our_group["Instances"]:
        if (
            instance["LifecycleState"] == "InService"
            and instance["HealthStatus"] == "Healthy"
        ):
            instance_ids.append(instance["InstanceId"])

    return instance_ids


def get_instances_addresses(ec2_client, instance_ids):
    ec2_response = ec2_client.describe_instances(InstanceIds=instance_ids)
    instance_ips = []
    for instances in ec2_response["Reservations"]:
        for ip in instances["Instances"]:
            instance_ips.append(ip["PrivateIpAddress"])
    return instance_ips


def prepare_autoscaling_group_and_get_hostnames(count):
    asg_client = boto3.client("autoscaling", region_name="us-east-1")
    asg_client.set_desired_capacity(
        AutoScalingGroupName=JEPSEN_GROUP_NAME, DesiredCapacity=count
    )

    instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
    counter = 0
    while len(instances) < count:
        time.sleep(5)
        instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
        counter += 1
        if counter > 30:
            raise Exception("Cannot wait autoscaling group")

    ec2_client = boto3.client("ec2", region_name="us-east-1")
    return get_instances_addresses(ec2_client, instances)


def clear_autoscaling_group():
    asg_client = boto3.client("autoscaling", region_name="us-east-1")
    asg_client.set_desired_capacity(
        AutoScalingGroupName=JEPSEN_GROUP_NAME, DesiredCapacity=0
    )
    instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
    counter = 0
    while len(instances) > 0:
        time.sleep(5)
        instances = get_autoscaling_group_instances_ids(asg_client, JEPSEN_GROUP_NAME)
        counter += 1
        if counter > 30:
            raise Exception("Cannot wait autoscaling group")


def save_nodes_to_file(instances: List[Any], temp_path: Path) -> Path:
    nodes_path = temp_path / "nodes.txt"
    with open(nodes_path, "w") as f:
        f.write("\n".join(instances))
        f.flush()
    return nodes_path


def get_run_command(
    ssh_auth_sock,
    ssh_sock_dir,
    pr_info,
    nodes_path,
    repo_path,
    build_url,
    result_path,
    extra_args,
    docker_image,
):
    return (
        f"docker run --network=host -v '{ssh_sock_dir}:{ssh_sock_dir}' -e SSH_AUTH_SOCK={ssh_auth_sock} "
        f"-e PR_TO_TEST={pr_info.number} -e SHA_TO_TEST={pr_info.sha} -v '{nodes_path}:/nodes.txt' -v {result_path}:/test_output "
        f"-e 'CLICKHOUSE_PACKAGE={build_url}' -v '{repo_path}:/ch' -e 'CLICKHOUSE_REPO_PATH=/ch' -e NODES_USERNAME=ubuntu {extra_args} {docker_image}"
    )


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        prog="Jepsen Check",
        description="Check that uses Jepsen. Both Keeper and Server can be tested.",
    )
    parser.add_argument(
        "program", help='What should be tested. Valid values "keeper", "server"'
    )
    args = parser.parse_args()

    if args.program != "server" and args.program != "keeper":
        logging.warning("Invalid argument '%s'", args.program)
        sys.exit(0)

    stopwatch = Stopwatch()
    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()

    logging.info(
        "Start at PR number %s, commit sha %s labels %s",
        pr_info.number,
        pr_info.sha,
        pr_info.labels,
    )

    if pr_info.number != 0 and "jepsen-test" not in pr_info.labels:
        logging.info("Not jepsen test label in labels list, skipping")
        sys.exit(0)

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    check_name = KEEPER_CHECK_NAME if args.program == "keeper" else SERVER_CHECK_NAME

    rerun_helper = RerunHelper(commit, check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    if not os.path.exists(TEMP_PATH):
        os.makedirs(TEMP_PATH)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    instances = prepare_autoscaling_group_and_get_hostnames(
        KEEPER_DESIRED_INSTANCE_COUNT
        if args.program == "keeper"
        else SERVER_DESIRED_INSTANCE_COUNT
    )
    nodes_path = save_nodes_to_file(
        instances[:KEEPER_DESIRED_INSTANCE_COUNT], temp_path
    )

    # always use latest
    docker_image = KEEPER_IMAGE_NAME if args.program == "keeper" else SERVER_IMAGE_NAME

    build_name = get_build_name_for_check(check_name)

    release_or_pr, _ = get_release_or_pr(pr_info, get_version_from_repo())

    # This check run separately from other checks because it requires exclusive
    # run (see .github/workflows/jepsen.yml) So we cannot add explicit
    # dependency on a build job and using busy loop on it's results. For the
    # same reason we are using latest docker image.
    build_url = (
        f"{S3_DOWNLOAD}/{S3_BUILDS_BUCKET}/{release_or_pr}/{pr_info.sha}/"
        f"{build_name}/clickhouse"
    )
    head = requests.head(build_url)
    counter = 0
    while head.status_code != 200:
        time.sleep(10)
        head = requests.head(build_url)
        counter += 1
        if counter >= 180:
            logging.warning("Cannot fetch build in 30 minutes, exiting")
            sys.exit(0)

    extra_args = ""
    if args.program == "server":
        extra_args = f"-e KEEPER_NODE={instances[-1]}"

    with SSHKey(key_value=get_parameter_from_ssm("jepsen_ssh_key") + "\n"):
        ssh_auth_sock = os.environ["SSH_AUTH_SOCK"]
        auth_sock_dir = os.path.dirname(ssh_auth_sock)
        cmd = get_run_command(
            ssh_auth_sock,
            auth_sock_dir,
            pr_info,
            nodes_path,
            REPO_COPY,
            build_url,
            result_path,
            extra_args,
            docker_image,
        )
        logging.info("Going to run jepsen: %s", cmd)

        run_log_path = temp_path / "run.log"

        with TeePopen(cmd, run_log_path) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
            else:
                logging.info("Run failed")

    status = "success"
    description = "No invalid analysis found ヽ(‘ー`)ノ"
    jepsen_log_path = result_path / "jepsen_run_all_tests.log"
    additional_data = []
    try:
        test_result = _parse_jepsen_output(jepsen_log_path)
        if any(r.status == "FAIL" for r in test_result):
            status = "failure"
            description = "Found invalid analysis (ﾉಥ益ಥ）ﾉ ┻━┻"

        compress_fast(result_path / "store", result_path / "jepsen_store.tar.zst")
        additional_data.append(result_path / "jepsen_store.tar.zst")
    except Exception as ex:
        print("Exception", ex)
        status = "failure"
        description = "No Jepsen output log"
        test_result = [TestResult("No Jepsen output log", "FAIL")]

    s3_helper = S3Helper()
    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_result,
        [run_log_path] + additional_data,
        check_name,
    )

    print(f"::notice ::Report url: {report_url}")
    post_commit_status(
        commit, status, report_url, description, check_name, pr_info, dump_to_file=True
    )

    ch_helper = ClickHouseHelper()
    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_result,
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        check_name,
    )
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)
    clear_autoscaling_group()


if __name__ == "__main__":
    main()
