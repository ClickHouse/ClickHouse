#!/usr/bin/env python3

import argparse
import json
import sys
import time
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import boto3  # type: ignore
import requests  # type: ignore
import jwt


def get_key_and_app_from_aws() -> Tuple[str, int]:
    secret_name = "clickhouse_github_secret_key"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    data = json.loads(get_secret_value_response["SecretString"])
    return data["clickhouse-app-key"], int(data["clickhouse-app-id"])


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


@dataclass
class CachedToken:
    time: int
    value: str
    updating: bool = False


cached_token = CachedToken(0, "")


def get_cached_access_token() -> str:
    if time.time() - 550 < cached_token.time or cached_token.updating:
        return cached_token.value
    # Indicate that the value is updating now, so the cached value can be
    # used. The first setting and close-to-ttl are not counted as update
    if cached_token.time != 0 or time.time() - 590 < cached_token.time:
        cached_token.updating = True
    private_key, app_id = get_key_and_app_from_aws()
    payload = {
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + (10 * 60),
        "iss": app_id,
    }

    encoded_jwt = jwt.encode(payload, private_key, algorithm="RS256")
    installation_id = get_installation_id(encoded_jwt)
    cached_token.time = int(time.time())
    cached_token.value = get_access_token(encoded_jwt, installation_id)
    cached_token.updating = False
    return cached_token.value


@dataclass
class CachedInstances:
    time: int
    value: dict
    updating: bool = False


cached_instances = CachedInstances(0, {})


def get_cached_instances() -> dict:
    """return cached instances description with updating it once per five minutes"""
    if time.time() - 250 < cached_instances.time or cached_instances.updating:
        return cached_instances.value
    # Indicate that the value is updating now, so the cached value can be
    # used. The first setting and close-to-ttl are not counted as update
    if cached_instances.time != 0 or time.time() - 300 < cached_instances.time:
        cached_instances.updating = True
    ec2_client = boto3.client("ec2")
    instances_response = ec2_client.describe_instances(
        Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
    )
    cached_instances.time = int(time.time())
    cached_instances.value = {
        instance["InstanceId"]: instance
        for reservation in instances_response["Reservations"]
        for instance in reservation["Instances"]
    }
    cached_instances.updating = False
    return cached_instances.value


RunnerDescription = namedtuple(
    "RunnerDescription", ["id", "name", "tags", "offline", "busy"]
)
RunnerDescriptions = List[RunnerDescription]


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


def how_many_instances_to_kill(event_data: dict) -> Dict[str, int]:
    data_array = event_data["CapacityToTerminate"]
    to_kill_by_zone = {}  # type: Dict[str, int]
    for av_zone in data_array:
        zone_name = av_zone["AvailabilityZone"]
        to_kill = av_zone["Capacity"]
        if zone_name not in to_kill_by_zone:
            to_kill_by_zone[zone_name] = 0

        to_kill_by_zone[zone_name] += to_kill

    return to_kill_by_zone


def get_candidates_to_be_killed(event_data: dict) -> Dict[str, List[str]]:
    data_array = event_data["Instances"]
    instances_by_zone = {}  # type: Dict[str, List[str]]
    for instance in data_array:
        zone_name = instance["AvailabilityZone"]
        instance_id = instance["InstanceId"]  # type: str
        if zone_name not in instances_by_zone:
            instances_by_zone[zone_name] = []
        instances_by_zone[zone_name].append(instance_id)

    return instances_by_zone


def main(access_token: str, event: dict) -> Dict[str, List[str]]:
    start = time.time()
    print("Got event", json.dumps(event, sort_keys=True).replace("\n", ""))
    to_kill_by_zone = how_many_instances_to_kill(event)
    instances_by_zone = get_candidates_to_be_killed(event)
    # Getting ASG and instances' descriptions from the API
    # We don't kill instances that alive for less than 10 minutes, since they
    # could be not in the GH active runners yet
    print(f"Check other hosts from the same ASG {event['AutoScalingGroupName']}")
    asg_client = boto3.client("autoscaling")
    as_groups_response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[event["AutoScalingGroupName"]]
    )
    assert len(as_groups_response["AutoScalingGroups"]) == 1
    asg = as_groups_response["AutoScalingGroups"][0]
    asg_instance_ids = [instance["InstanceId"] for instance in asg["Instances"]]
    instance_descriptions = get_cached_instances()
    # The instances launched less than 10 minutes ago
    immune_ids = [
        instance["InstanceId"]
        for instance in instance_descriptions.values()
        if start - instance["LaunchTime"].timestamp() < 600
    ]
    # if the ASG's instance ID not in instance_descriptions, it's most probably
    # is not cached yet, so we must mark it as immuned
    immune_ids.extend(
        iid for iid in asg_instance_ids if iid not in instance_descriptions
    )
    print("Time spent on the requests to AWS: ", time.time() - start)

    runners = list_runners(access_token)
    runner_ids = set(runner.name for runner in runners)
    # We used to delete potential hosts to terminate from GitHub runners pool,
    # but the documentation states:
    # --- Returning an instance first in the response data does not guarantee its termination
    # so they will be cleaned out by ci_runners_metrics_lambda eventually

    instances_to_kill = []
    total_to_kill = 0
    for zone, num_to_kill in to_kill_by_zone.items():
        candidates = instances_by_zone[zone]
        total_to_kill += num_to_kill
        if num_to_kill > len(candidates):
            raise Exception(
                f"Required to kill {num_to_kill}, but have only {len(candidates)}"
                f" candidates in AV {zone}"
            )

        delete_for_av = []  # type: RunnerDescriptions
        for candidate in candidates:
            if candidate in immune_ids:
                print(
                    f"Candidate {candidate} started less than 10 minutes ago, won't touch a child"
                )
                break
            if candidate not in runner_ids:
                print(
                    f"Candidate {candidate} was not in runners list, simply delete it"
                )
                instances_to_kill.append(candidate)
                break
            if len(delete_for_av) + len(instances_to_kill) == num_to_kill:
                break
            if candidate in instances_to_kill:
                continue

            for runner in runners:
                if runner.name == candidate:
                    if not runner.busy:
                        print(
                            f"Runner {runner.name} is not busy and can be deleted from AV {zone}"
                        )
                        delete_for_av.append(runner)
                    else:
                        print(f"Runner {runner.name} is busy, not going to delete it")
                    break

        if len(delete_for_av) < num_to_kill:
            print(
                f"Checked all candidates for av {zone}, get to delete "
                f"{len(delete_for_av)}, but still cannot get required {num_to_kill}"
            )

        instances_to_kill += [runner.name for runner in delete_for_av]

    if len(instances_to_kill) < total_to_kill:
        for instance in asg_instance_ids:
            if instance in immune_ids:
                continue
            for runner in runners:
                if runner.name == instance and not runner.busy:
                    print(f"Runner {runner.name} is not busy and can be deleted")
                    instances_to_kill.append(runner.name)

            if total_to_kill <= len(instances_to_kill):
                print("Got enough instances to kill")
                break

    response = {"InstanceIDs": instances_to_kill}
    print("Got instances to kill: ", response)
    print("Time spent on the request: ", time.time() - start)
    return response


def handler(event: dict, context: Any) -> Dict[str, List[str]]:
    return main(get_cached_access_token(), event)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get list of runners and their states")
    parser.add_argument(
        "-p", "--private-key-path", help="Path to file with private key"
    )
    parser.add_argument("-k", "--private-key", help="Private key")
    parser.add_argument(
        "-a", "--app-id", type=int, help="GitHub application ID", required=True
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
    else:
        with open(args.private_key_path, "r") as key_file:
            private_key = key_file.read()

    sample_event = {
        "AutoScalingGroupARN": "arn:aws:autoscaling:us-east-1:<account-id>:autoScalingGroup:d4738357-2d40-4038-ae7e-b00ae0227003:autoScalingGroupName/my-asg",
        "AutoScalingGroupName": "my-asg",
        "CapacityToTerminate": [
            {
                "AvailabilityZone": "us-east-1b",
                "Capacity": 1,
                "InstanceMarketOption": "OnDemand",
            },
            {
                "AvailabilityZone": "us-east-1c",
                "Capacity": 2,
                "InstanceMarketOption": "OnDemand",
            },
        ],
        "Instances": [
            {
                "AvailabilityZone": "us-east-1b",
                "InstanceId": "i-08d0b3c1a137e02a5",
                "InstanceType": "t2.nano",
                "InstanceMarketOption": "OnDemand",
            },
            {
                "AvailabilityZone": "us-east-1c",
                "InstanceId": "ip-172-31-45-253.eu-west-1.compute.internal",
                "InstanceType": "t2.nano",
                "InstanceMarketOption": "OnDemand",
            },
            {
                "AvailabilityZone": "us-east-1c",
                "InstanceId": "ip-172-31-27-227.eu-west-1.compute.internal",
                "InstanceType": "t2.nano",
                "InstanceMarketOption": "OnDemand",
            },
            {
                "AvailabilityZone": "us-east-1c",
                "InstanceId": "ip-172-31-45-253.eu-west-1.compute.internal",
                "InstanceType": "t2.nano",
                "InstanceMarketOption": "OnDemand",
            },
        ],
        "Cause": "SCALE_IN",
    }

    payload = {
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + (10 * 60),
        "iss": args.app_id,
    }

    encoded_jwt = jwt.encode(payload, private_key, algorithm="RS256")
    installation_id = get_installation_id(encoded_jwt)
    access_token = get_access_token(encoded_jwt, args.app_id)

    main(access_token, sample_event)
