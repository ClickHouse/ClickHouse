import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, List

import boto3
from tests.ci.ssh import SSHKey

from ci.defs.defs import JobNames
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Shell, Utils

JEPSEN_GROUP_NAME = "jepsen_group"

KEEPER_DESIRED_INSTANCE_COUNT = 3
SERVER_DESIRED_INSTANCE_COUNT = 4

KEEPER_IMAGE_NAME = "clickhouse/keeper-jepsen-test"
KEEPER_CHECK_NAME = JobNames.JEPSEN_KEEPER

SERVER_IMAGE_NAME = "clickhouse/server-jepsen-test"
SERVER_CHECK_NAME = JobNames.JEPSEN_SERVER

SUCCESSFUL_TESTS_ANCHOR = "# Successful tests"
INTERMINATE_TESTS_ANCHOR = "# Indeterminate tests"
CRASHED_TESTS_ANCHOR = "# Crashed tests"
FAILED_TESTS_ANCHOR = "# Failed tests"


def read_build_urls(path, build_name: str):
    artifact_report = path / f"artifact_report_build_{build_name}.json"
    if artifact_report.is_file():
        with open(artifact_report, "r", encoding="utf-8") as f:
            return json.load(f)["build_urls"]
    return []


def _parse_jepsen_output(path: Path):
    test_results = []
    current_type = ""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if SUCCESSFUL_TESTS_ANCHOR in line:
                current_type = Result.StatusExtended.OK
            elif INTERMINATE_TESTS_ANCHOR in line or CRASHED_TESTS_ANCHOR in line:
                current_type = Result.StatusExtended.ERROR
            elif FAILED_TESTS_ANCHOR in line:
                current_type = Result.StatusExtended.FAIL

            if (
                line.startswith("store/clickhouse") or line.startswith("clickhouse")
            ) and current_type:
                test_results.append(Result(line.strip(), current_type))

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
        f"-e PR_TO_TEST={pr_info.pr_number} -e SHA_TO_TEST={pr_info.sha} -v '{nodes_path}:/nodes.txt' -v {result_path}:/test_output "
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

    stopwatch = Utils.Stopwatch()
    temp_path = Path(Utils.cwd()) / "ci/tmp"
    temp_path.mkdir(parents=True, exist_ok=True)

    info = Info()

    logging.info(
        "Start at PR number %s, commit sha %s labels %s",
        info.pr_number,
        info.sha,
        info.pr_labels,
    )

    if info.pr_number != 0 and "jepsen-test" not in info.pr_labels:
        logging.info("Not jepsen test label in labels list, skipping")
        sys.exit(0)

    temp_path.mkdir(parents=True, exist_ok=True)

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

    # build amd_binary assumed to be always ready on the master as it's part of the merge queue workflow
    urls = read_build_urls(temp_path, "amd_binary")
    build_url = None
    for url in urls:
        if url.endswith("clickhouse"):
            build_url = url
    assert build_url, "No build url found in the report"

    extra_args = ""
    if args.program == "server":
        extra_args = f"-e KEEPER_NODE={instances[-1]}"

    ssh_key_secret = Secret.Config(
        name="jepsen_ssh_key", type=Secret.Type.AWS_SSM_PARAMETER
    )
    with SSHKey(key_value=ssh_key_secret.get_value() + "\n"):
        ssh_auth_sock = os.environ["SSH_AUTH_SOCK"]
        auth_sock_dir = os.path.dirname(ssh_auth_sock)
        cmd = get_run_command(
            ssh_auth_sock,
            auth_sock_dir,
            info,
            nodes_path,
            Utils.cwd(),
            build_url,
            result_path,
            extra_args,
            docker_image,
        )
        logging.info("Going to run jepsen: %s", cmd)

        if Shell.run(cmd) != 0:
            logging.error("Jepsen run command failed")

    clear_autoscaling_group()

    status = Result.Status.SUCCESS
    description = "No invalid analysis found ヽ(‘ー`)ノ"
    jepsen_log_path = result_path / "jepsen_run_all_tests.log"
    additional_data = []
    try:
        test_result = _parse_jepsen_output(jepsen_log_path)
        if len(test_result) == 0:
            status = Result.Status.FAILED
            description = "No test results found"
        elif any(r.status == "FAIL" for r in test_result):
            status = Result.Status.FAILED
            description = "Found invalid analysis (ﾉಥ益ಥ）ﾉ ┻━┻"

        additional_data.append(Utils.compress_zst(result_path / "store"))
    except Exception as ex:
        print("Exception", ex)
        status = Result.Status.FAILED
        description = "No Jepsen output log"
        test_result = [Result("No Jepsen output log", "FAIL")]

    Result.create_from(
        results=test_result,
        status=status if not test_result else "",
        stopwatch=stopwatch,
        files=additional_data,
        info=description,
    ).complete_job()


if __name__ == "__main__":
    main()
