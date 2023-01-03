#!/usr/bin/env python3
"""
Lambda function to:
    - calculate number of running runners
    - cleaning dead runners from GitHub
    - terminating stale lost runners in EC2
"""

import argparse
import sys
import json
import time
from collections import namedtuple
from datetime import datetime
from typing import Dict, List, Tuple

import jwt
import requests  # type: ignore
import boto3  # type: ignore
from botocore.exceptions import ClientError  # type: ignore

UNIVERSAL_LABEL = "universal"
RUNNER_TYPE_LABELS = [
    "builder",
    "func-tester",
    "func-tester-aarch64",
    "fuzzer-unit-tester",
    "stress-tester",
    "style-checker",
    "style-checker-aarch64",
]

RunnerDescription = namedtuple(
    "RunnerDescription", ["id", "name", "tags", "offline", "busy"]
)
RunnerDescriptions = List[RunnerDescription]


def get_dead_runners_in_ec2(runners: RunnerDescriptions) -> RunnerDescriptions:
    ids = {
        runner.name: runner
        for runner in runners
        # Only `i-deadbead123` are valid names for an instance ID
        if runner.offline and not runner.busy and runner.name.startswith("i-")
    }
    if not ids:
        return []

    result_to_delete = [
        runner
        for runner in runners
        if not ids.get(runner.name) and runner.offline and not runner.busy
    ]

    client = boto3.client("ec2")

    i = 0
    inc = 100

    print("Checking ids", ids.keys())
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
        Filters=[{"Name": "tag-key", "Values": ["github:runner-type"]}]
    )["Reservations"]
    lost_instances = []
    # Here we refresh the runners to get the most recent state
    now = datetime.now().timestamp()

    for reservation in reservations:
        for instance in reservation["Instances"]:
            # Do not consider instances started 20 minutes ago as problematic
            if now - instance["LaunchTime"].timestamp() < 1200:
                continue

            runner_type = [
                tag["Value"]
                for tag in instance["Tags"]
                if tag["Key"] == "github:runner-type"
            ][0]
            # If there's no necessary labels in runner type it's fine
            if not (
                UNIVERSAL_LABEL in runner_type or runner_type in RUNNER_TYPE_LABELS
            ):
                continue

            if instance["State"]["Name"] == "running" and (
                not [
                    runner
                    for runner in runners
                    if runner.name == instance["InstanceId"]
                ]
            ):
                lost_instances.append(instance)

    return lost_instances


def get_key_and_app_from_aws() -> Tuple[str, int]:
    secret_name = "clickhouse_github_secret_key"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    data = json.loads(get_secret_value_response["SecretString"])
    return data["clickhouse-app-key"], int(data["clickhouse-app-id"])


def handler(event, context):
    private_key, app_id = get_key_and_app_from_aws()
    main(private_key, app_id, True, True)


def get_installation_id(jwt_token: str) -> int:
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    data = response.json()
    for installation in data:
        if installation["account"]["login"] == "ClickHouse":
            installation_id = installation["id"]
            break

    return installation_id  # type: ignore


def get_access_token(jwt_token: str, installation_id: int) -> str:
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.post(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        headers=headers,
    )
    response.raise_for_status()
    data = response.json()
    return data["token"]  # type: ignore


def list_runners(access_token: str) -> RunnerDescriptions:
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    per_page = 100
    response = requests.get(
        f"https://api.github.com/orgs/ClickHouse/actions/runners?per_page={per_page}",
        headers=headers,
    )
    response.raise_for_status()
    data = response.json()
    total_runners = data["total_count"]
    print("Expected total runners", total_runners)
    runners = data["runners"]

    # round to 0 for 0, 1 for 1..100, but to 2 for 101..200
    total_pages = (total_runners - 1) // per_page + 1

    print("Total pages", total_pages)
    for i in range(2, total_pages + 1):
        response = requests.get(
            "https://api.github.com/orgs/ClickHouse/actions/runners"
            f"?page={i}&per_page={per_page}",
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
        runners += data["runners"]

    print("Total runners", len(runners))
    result = []
    for runner in runners:
        tags = [tag["name"] for tag in runner["labels"]]
        desc = RunnerDescription(
            id=runner["id"],
            name=runner["name"],
            tags=tags,
            offline=runner["status"] == "offline",
            busy=runner["busy"],
        )
        result.append(desc)

    return result


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
    listed_runners: RunnerDescriptions, namespace: str
) -> None:
    client = boto3.client("cloudwatch")
    metrics_data = []
    busy_runners = sum(
        1 for runner in listed_runners if runner.busy and not runner.offline
    )
    metrics_data.append(
        {
            "MetricName": "BusyRunners",
            "Value": busy_runners,
            "Unit": "Count",
        }
    )
    total_active_runners = sum(1 for runner in listed_runners if not runner.offline)
    metrics_data.append(
        {
            "MetricName": "ActiveRunners",
            "Value": total_active_runners,
            "Unit": "Count",
        }
    )
    total_runners = len(listed_runners)
    metrics_data.append(
        {
            "MetricName": "TotalRunners",
            "Value": total_runners,
            "Unit": "Count",
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
    github_secret_key: str,
    github_app_id: int,
    push_to_cloudwatch: bool,
    delete_offline_runners: bool,
) -> None:
    payload = {
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + (10 * 60),
        "iss": github_app_id,
    }

    encoded_jwt = jwt.encode(payload, github_secret_key, algorithm="RS256")
    installation_id = get_installation_id(encoded_jwt)
    access_token = get_access_token(encoded_jwt, installation_id)
    gh_runners = list_runners(access_token)
    grouped_runners = group_runners_by_tag(gh_runners)
    for group, group_runners in grouped_runners.items():
        if push_to_cloudwatch:
            print(f"Pushing metrics for group '{group}'")
            push_metrics_to_cloudwatch(group_runners, "RunnersMetrics/" + group)
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

    main(private_key, args.app_id, args.push_to_cloudwatch, args.delete_offline)
