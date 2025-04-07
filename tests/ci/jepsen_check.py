#!/usr/bin/env python3

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, List

import boto3  # type: ignore

from build_download_helper import read_build_urls
from ci_config import CI
from compress_files import compress_fast
from env_helper import REPO_COPY, REPORT_PATH, TEMP_PATH
from get_robot_token import get_parameter_from_ssm
from pr_info import PRInfo
from report import FAILURE, SUCCESS, JobReport, TestResult, TestResults
from ssh import SSHKey
from stopwatch import Stopwatch
from tee_popen import TeePopen

JEPSEN_GROUP_NAME = "jepsen_group"

KEEPER_DESIRED_INSTANCE_COUNT = 3
SERVER_DESIRED_INSTANCE_COUNT = 4

KEEPER_IMAGE_NAME = "clickhouse/keeper-jepsen-test"
KEEPER_CHECK_NAME = CI.JobNames.JEPSEN_KEEPER

SERVER_IMAGE_NAME = "clickhouse/server-jepsen-test"
SERVER_CHECK_NAME = CI.JobNames.JEPSEN_SERVER

SUCCESSFUL_TESTS_ANCHOR = "# Successful tests"
INTERMINATE_TESTS_ANCHOR = "# Indeterminate tests"
CRASHED_TESTS_ANCHOR = "# Crashed tests"
FAILED_TESTS_ANCHOR = "# Failed tests"


def _parse_jepsen_output(path: Path) -> TestResults:
    test_results = []  # type: TestResults
    current_type = ""
    with open(path, "r", encoding="utf-8") as f:
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
            raise RuntimeError("Cannot wait autoscaling group")

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
            raise RuntimeError("Cannot wait autoscaling group")


def save_nodes_to_file(instances: List[Any], temp_path: Path) -> Path:
    nodes_path = temp_path / "nodes.txt"
    with open(nodes_path, "w", encoding="utf-8") as f:
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

    if args.program not in ("server", "keeper"):
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

    check_name = KEEPER_CHECK_NAME if args.program == "keeper" else SERVER_CHECK_NAME

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

    # binary_release assumed to be always ready on the master as it's part of the merge queue workflow
    build_name = CI.get_required_build_name(check_name)
    urls = read_build_urls(build_name, REPORT_PATH)
    build_url = None
    for url in urls:
        if url.endswith("clickhouse"):
            build_url = url
    assert build_url, "No build url found in the report"

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

    status = SUCCESS
    description = "No invalid analysis found ヽ(‘ー`)ノ"
    jepsen_log_path = result_path / "jepsen_run_all_tests.log"
    additional_data = []
    try:
        test_result = _parse_jepsen_output(jepsen_log_path)
        if len(test_result) == 0:
            status = FAILURE
            description = "No test results found"
        elif any(r.status == "FAIL" for r in test_result):
            status = FAILURE
            description = "Found invalid analysis (ﾉಥ益ಥ）ﾉ ┻━┻"

        compress_fast(result_path / "store", result_path / "jepsen_store.tar.zst")
        additional_data.append(result_path / "jepsen_store.tar.zst")
    except Exception as ex:
        print("Exception", ex)
        status = FAILURE
        description = "No Jepsen output log"
        test_result = [TestResult("No Jepsen output log", "FAIL")]

    JobReport(
        description=description,
        test_results=test_result,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[run_log_path] + additional_data,
        check_name=check_name,
    ).dump()

    clear_autoscaling_group()


if __name__ == "__main__":
    main()
