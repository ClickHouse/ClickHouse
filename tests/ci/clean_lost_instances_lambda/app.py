#!/usr/bin/env python3
"""
Lambda function to:
    - calculate number of running runners
    - cleaning dead runners from GitHub
    - terminating stale lost runners in EC2
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import boto3  # type: ignore
import requests
from botocore.exceptions import ClientError  # type: ignore
from lambda_shared import (
    RUNNER_TYPE_LABELS,
    RunnerDescription,
    RunnerDescriptions,
    list_runners,
)
from lambda_shared.token import (
    get_access_token_by_key_app,
    get_cached_access_token,
    get_key_and_app_from_aws,
)

UNIVERSAL_LABEL = "universal"


@dataclass
class LostInstance:
    counter: int
    seen: datetime

    def set_offline(self) -> None:
        now = datetime.now()
        if now.timestamp() <= self.seen.timestamp() + 120:
            # the instance is offline for more than 2 minutes, so we increase
            # the counter
            self.counter += 1
        else:
            self.counter = 1
        self.seen = now

    @property
    def recently_offline(self) -> bool:
        """Returns True if the instance has been seen less than 5 minutes ago"""
        return datetime.now().timestamp() <= self.seen.timestamp() + 300

    @property
    def stable_offline(self) -> bool:
        return self.counter >= 3


LOST_INSTANCES = {}  # type: Dict["str", LostInstance]


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


def handler(event, context):
    _ = event
    _ = context
    main(get_cached_access_token(), True)


def delete_runner(access_token: str, runner: RunnerDescription) -> bool:
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.delete(
        f"https://api.github.com/orgs/ClickHouse/actions/runners/{runner.id}",
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    print(f"Response code deleting {runner.name} is {response.status_code}")
    return bool(response.status_code == 204)


def get_lost_ec2_instances(runners: RunnerDescriptions) -> List[str]:
    global LOST_INSTANCES
    now = datetime.now()
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
    offline_runner_names = {
        runner.name for runner in runners if runner.offline and not runner.busy
    }
    runner_names = {runner.name for runner in runners}

    def offline_instance(iid: str) -> None:
        if iid in LOST_INSTANCES:
            LOST_INSTANCES[iid].set_offline()
            return
        LOST_INSTANCES[iid] = LostInstance(1, now)

    for instance in instances:
        # Do not consider instances started 20 minutes ago as problematic
        if now.timestamp() - instance["LaunchTime"].timestamp() < 1200:
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
            offline_instance(instance["InstanceId"])
            continue

        if (
            instance["State"]["Name"] == "running"
            and not instance["InstanceId"] in runner_names
        ):
            offline_instance(instance["InstanceId"])

    instance_ids = [instance["InstanceId"] for instance in instances]
    # clean out long unseen instances
    LOST_INSTANCES = {
        instance_id: stats
        for instance_id, stats in LOST_INSTANCES.items()
        if stats.recently_offline and instance_id in instance_ids
    }
    print("The remained LOST_INSTANCES: ", LOST_INSTANCES)

    return [
        instance_id
        for instance_id, stats in LOST_INSTANCES.items()
        if stats.stable_offline
    ]


def continue_lifecycle_hooks(delete_offline_runners: bool) -> None:
    """The function to trigger CONTINUE for instances' lifectycle hooks"""
    client = boto3.client("ec2")
    reservations = client.describe_instances(
        Filters=[
            {"Name": "tag-key", "Values": ["github:runner-type"]},
            {"Name": "instance-state-name", "Values": ["shutting-down", "terminated"]},
        ],
    )["Reservations"]
    # flatten the reservation into instances
    terminated_instances = [
        instance["InstanceId"]
        for reservation in reservations
        for instance in reservation["Instances"]
    ]

    asg_client = boto3.client("autoscaling")
    as_groups = asg_client.describe_auto_scaling_groups(
        Filters=[{"Name": "tag-key", "Values": ["github:runner-type"]}]
    )["AutoScalingGroups"]
    for asg in as_groups:
        lifecycle_hooks = [
            lch
            for lch in asg_client.describe_lifecycle_hooks(
                AutoScalingGroupName=asg["AutoScalingGroupName"]
            )["LifecycleHooks"]
            if lch["LifecycleTransition"] == "autoscaling:EC2_INSTANCE_TERMINATING"
        ]
        if not lifecycle_hooks:
            continue
        for instance in asg["Instances"]:
            continue_instance = False
            if instance["LifecycleState"] == "Terminating:Wait":
                if instance["HealthStatus"] == "Unhealthy":
                    print(f"The instance {instance['InstanceId']} is Unhealthy")
                    continue_instance = True
                elif (
                    instance["HealthStatus"] == "Healthy"
                    and instance["InstanceId"] in terminated_instances
                ):
                    print(
                        f"The instance {instance['InstanceId']} is already terminated"
                    )
                    continue_instance = True
            if continue_instance:
                if delete_offline_runners:
                    for lch in lifecycle_hooks:
                        print(f"Continue lifecycle hook {lch['LifecycleHookName']}")
                        asg_client.complete_lifecycle_action(
                            LifecycleHookName=lch["LifecycleHookName"],
                            AutoScalingGroupName=asg["AutoScalingGroupName"],
                            LifecycleActionResult="CONTINUE",
                            InstanceId=instance["InstanceId"],
                        )


def main(
    access_token: str,
    delete_offline_runners: bool,
) -> None:
    gh_runners = list_runners(access_token)

    dead_runners = get_dead_runners_in_ec2(gh_runners)
    print("Runners in GH API to terminate: ", [runner.name for runner in dead_runners])
    if delete_offline_runners and dead_runners:
        print("Going to delete offline runners")
        for runner in dead_runners:
            print("Deleting runner", runner)
            delete_runner(access_token, runner)
    elif dead_runners:
        print("Would delete dead runners: ", dead_runners)

    lost_instances = get_lost_ec2_instances(gh_runners)
    print("Instances to terminate: ", lost_instances)
    if delete_offline_runners:
        if lost_instances:
            print("Going to terminate lost instances")
            boto3.client("ec2").terminate_instances(InstanceIds=lost_instances)

    continue_lifecycle_hooks(delete_offline_runners)


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
        with open(args.private_key_path, "r", encoding="utf-8") as key_file:
            private_key = key_file.read()
    else:
        print("Attempt to get key and id from AWS secret manager")
        private_key, args.app_id = get_key_and_app_from_aws()

    token = get_access_token_by_key_app(private_key, args.app_id)

    main(token, args.delete_offline)
