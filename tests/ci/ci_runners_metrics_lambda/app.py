#!/usr/bin/env python3
"""
Lambda function to:
    - calculate number of running runners
    - cleaning dead runners from GitHub
    - terminating stale lost runners in EC2
"""

import argparse
import sys
from datetime import datetime
from typing import Dict, List

import requests  # type: ignore
import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

from lambda_shared import (
    RUNNER_TYPE_LABELS,
    RunnerDescription,
    RunnerDescriptions,
    list_runners,
)
from lambda_shared.token import (
    get_cached_access_token,
    get_key_and_app_from_aws,
    get_access_token_by_key_app,
)

UNIVERSAL_LABEL = "universal"


def get_dead_runners_in_ec2(runners: RunnerDescriptions) -> RunnerDescriptions:
    """Returns instances that are offline/dead in EC2, or not found in EC2"""
    ids = {
        runner.name: runner
        for runner in runners
        # Only `i-deadbead123` are valid names for an instance ID
        if runner.name.startswith("i-") and runner.offline and not runner.busy
    }
    if not ids:
        return []

    # Delete all offline runners with wrong name
    result_to_delete = [
        runner
        for runner in runners
        if not ids.get(runner.name) and runner.offline and not runner.busy
    ]

    client = boto3.client("ec2")

    i = 0
    inc = 100

    print("Checking ids: ", " ".join(ids.keys()))
    instances_statuses = []
    while i < len(ids.keys()):
        try:
            instances_statuses.append(
                client.describe_instance_status(
                    InstanceIds=list(ids.keys())[i : i + inc]
                )
            )
            # It applied only if all ids exist in EC2
            i += inc
        except ClientError as e:
            # The list of non-existent instances is in the message:
            #   The instance IDs 'i-069b1c256c06cf4e3, i-0f26430432b044035,
            #   i-0faa2ff44edbc147e, i-0eccf2514585045ec, i-0ee4ee53e0daa7d4a,
            #   i-07928f15acd473bad, i-0eaddda81298f9a85' do not exist
            message = e.response["Error"]["Message"]
            if message.startswith("The instance IDs '") and message.endswith(
                "' do not exist"
            ):
                non_existent = message[18:-14].split(", ")
                for n in non_existent:
                    result_to_delete.append(ids.pop(n))
            else:
                raise

    found_instances = set([])
    print("Response", instances_statuses)
    for instances_status in instances_statuses:
        for instance_status in instances_status["InstanceStatuses"]:
            if instance_status["InstanceState"]["Name"] in ("pending", "running"):
                found_instances.add(instance_status["InstanceId"])

    print("Found instances", found_instances)
    for runner in result_to_delete:
        print("Instance", runner.name, "is not alive, going to remove it")
    for instance_id, runner in ids.items():
        if instance_id not in found_instances:
            print("Instance", instance_id, "is not found in EC2, going to remove it")
            result_to_delete.append(runner)
    return result_to_delete


def get_lost_ec2_instances(runners: RunnerDescriptions) -> List[dict]:
    client = boto3.client("ec2")
    reservations = client.describe_instances(
        Filters=[
            {"Name": "tag-key", "Values": ["github:runner-type"]},
            {"Name": "instance-state-name", "Values": ["pending", "running"]},
        ],
    )["Reservations"]
    # flatten the reservation into instances
    instances = [
        instance
        for reservation in reservations
        for instance in reservation["Instances"]
    ]
    lost_instances = []
    offline_runner_names = {
        runner.name for runner in runners if runner.offline and not runner.busy
    }
    runner_names = {runner.name for runner in runners}
    now = datetime.now().timestamp()

    for instance in instances:
        # Do not consider instances started 20 minutes ago as problematic
        if now - instance["LaunchTime"].timestamp() < 1200:
            continue

        runner_type = [
            tag["Value"]
            for tag in instance["Tags"]
            if tag["Key"] == "github:runner-type"
        ][0]
        # If there's no necessary labels in runner type it's fine
        if not (UNIVERSAL_LABEL in runner_type or runner_type in RUNNER_TYPE_LABELS):
            continue

        if instance["InstanceId"] in offline_runner_names:
            lost_instances.append(instance)
            continue

        if (
            instance["State"]["Name"] == "running"
            and not instance["InstanceId"] in runner_names
        ):
            lost_instances.append(instance)

    return lost_instances


def handler(event, context):
    main(get_cached_access_token(), True, True)


def group_runners_by_tag(
    listed_runners: RunnerDescriptions,
) -> Dict[str, RunnerDescriptions]:
    result = {}  # type: Dict[str, RunnerDescriptions]

    def add_to_result(tag, runner):
        if tag not in result:
            result[tag] = []
        result[tag].append(runner)

    for runner in listed_runners:
        if UNIVERSAL_LABEL in runner.tags:
            # Do not proceed other labels if UNIVERSAL_LABEL is included
            add_to_result(UNIVERSAL_LABEL, runner)
            continue

        for tag in runner.tags:
            if tag in RUNNER_TYPE_LABELS:
                add_to_result(tag, runner)
                break
        else:
            add_to_result("unlabeled", runner)
    return result


def push_metrics_to_cloudwatch(
    listed_runners: RunnerDescriptions, group_name: str
) -> None:
    client = boto3.client("cloudwatch")
    namespace = "RunnersMetrics"
    metrics_data = []
    busy_runners = sum(
        1 for runner in listed_runners if runner.busy and not runner.offline
    )
    dimensions = [{"Name": "group", "Value": group_name}]
    metrics_data.append(
        {
            "MetricName": "BusyRunners",
            "Value": busy_runners,
            "Unit": "Count",
            "Dimensions": dimensions,
        }
    )
    total_active_runners = sum(1 for runner in listed_runners if not runner.offline)
    metrics_data.append(
        {
            "MetricName": "ActiveRunners",
            "Value": total_active_runners,
            "Unit": "Count",
            "Dimensions": dimensions,
        }
    )
    total_runners = len(listed_runners)
    metrics_data.append(
        {
            "MetricName": "TotalRunners",
            "Value": total_runners,
            "Unit": "Count",
            "Dimensions": dimensions,
        }
    )
    if total_active_runners == 0:
        busy_ratio = 100.0
    else:
        busy_ratio = busy_runners / total_active_runners * 100

    metrics_data.append(
        {
            "MetricName": "BusyRunnersRatio",
            "Value": busy_ratio,
            "Unit": "Percent",
            "Dimensions": dimensions,
        }
    )

    client.put_metric_data(Namespace=namespace, MetricData=metrics_data)


def delete_runner(access_token: str, runner: RunnerDescription) -> bool:
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.delete(
        f"https://api.github.com/orgs/ClickHouse/actions/runners/{runner.id}",
        headers=headers,
    )
    response.raise_for_status()
    print(f"Response code deleting {runner.name} is {response.status_code}")
    return bool(response.status_code == 204)


def main(
    access_token: str,
    push_to_cloudwatch: bool,
    delete_offline_runners: bool,
) -> None:
    gh_runners = list_runners(access_token)
    grouped_runners = group_runners_by_tag(gh_runners)
    for group, group_runners in grouped_runners.items():
        if push_to_cloudwatch:
            print(f"Pushing metrics for group '{group}'")
            push_metrics_to_cloudwatch(group_runners, group)
        else:
            print(group, f"({len(group_runners)})")
            for runner in group_runners:
                print("\t", runner)

    if delete_offline_runners:
        print("Going to delete offline runners")
        dead_runners = get_dead_runners_in_ec2(gh_runners)
        for runner in dead_runners:
            print("Deleting runner", runner)
            delete_runner(access_token, runner)

        lost_instances = get_lost_ec2_instances(gh_runners)
        if lost_instances:
            print("Going to terminate lost runners")
            ids = [i["InstanceId"] for i in lost_instances]
            print("Terminating runners:", ids)
            boto3.client("ec2").terminate_instances(InstanceIds=ids)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get list of runners and their states")
    parser.add_argument(
        "-p", "--private-key-path", help="Path to file with private key"
    )
    parser.add_argument("-k", "--private-key", help="Private key")
    parser.add_argument(
        "-a", "--app-id", type=int, help="GitHub application ID", required=True
    )
    parser.add_argument(
        "--push-to-cloudwatch",
        action="store_true",
        help="Push metrics for active and busy runners to cloudwatch",
    )
    parser.add_argument(
        "--delete-offline", action="store_true", help="Remove offline runners"
    )

    args = parser.parse_args()

    if not args.private_key_path and not args.private_key:
        print(
            "Either --private-key-path or --private-key must be specified",
            file=sys.stderr,
        )

    if args.private_key_path and args.private_key:
        print(
            "Either --private-key-path or --private-key must be specified",
            file=sys.stderr,
        )

    if args.private_key:
        private_key = args.private_key
    elif args.private_key_path:
        with open(args.private_key_path, "r") as key_file:
            private_key = key_file.read()
    else:
        print("Attempt to get key and id from AWS secret manager")
        private_key, args.app_id = get_key_and_app_from_aws()

    token = get_access_token_by_key_app(private_key, args.app_id)

    main(token, args.push_to_cloudwatch, args.delete_offline)
