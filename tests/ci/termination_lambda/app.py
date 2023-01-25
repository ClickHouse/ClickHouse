#!/usr/bin/env python3

import requests
import argparse
import jwt
import sys
import json
import time
from collections import namedtuple


def get_key_and_app_from_aws():
    import boto3

    secret_name = "clickhouse_github_secret_key"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    data = json.loads(get_secret_value_response["SecretString"])
    return data["clickhouse-app-key"], int(data["clickhouse-app-id"])


def get_installation_id(jwt_token):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    data = response.json()
    return data[0]["id"]


def get_access_token(jwt_token, installation_id):
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
    return data["token"]


RunnerDescription = namedtuple(
    "RunnerDescription", ["id", "name", "tags", "offline", "busy"]
)


def list_runners(access_token):
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get(
        "https://api.github.com/orgs/ClickHouse/actions/runners?per_page=100",
        headers=headers,
    )
    response.raise_for_status()
    data = response.json()
    total_runners = data["total_count"]
    runners = data["runners"]

    total_pages = int(total_runners / 100 + 1)
    for i in range(2, total_pages + 1):
        response = requests.get(
            f"https://api.github.com/orgs/ClickHouse/actions/runners?page={i}&per_page=100",
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


def push_metrics_to_cloudwatch(listed_runners, namespace):
    import boto3

    client = boto3.client("cloudwatch")
    metrics_data = []
    busy_runners = sum(1 for runner in listed_runners if runner.busy)
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
        busy_ratio = 100
    else:
        busy_ratio = busy_runners / total_active_runners * 100

    metrics_data.append(
        {
            "MetricName": "BusyRunnersRatio",
            "Value": busy_ratio,
            "Unit": "Percent",
        }
    )

    client.put_metric_data(Namespace="RunnersMetrics", MetricData=metrics_data)


def how_many_instances_to_kill(event_data):
    data_array = event_data["CapacityToTerminate"]
    to_kill_by_zone = {}
    for av_zone in data_array:
        zone_name = av_zone["AvailabilityZone"]
        to_kill = av_zone["Capacity"]
        if zone_name not in to_kill_by_zone:
            to_kill_by_zone[zone_name] = 0

        to_kill_by_zone[zone_name] += to_kill
    return to_kill_by_zone


def get_candidates_to_be_killed(event_data):
    data_array = event_data["Instances"]
    instances_by_zone = {}
    for instance in data_array:
        zone_name = instance["AvailabilityZone"]
        instance_id = instance["InstanceId"]
        if zone_name not in instances_by_zone:
            instances_by_zone[zone_name] = []
        instances_by_zone[zone_name].append(instance_id)

    return instances_by_zone


def delete_runner(access_token, runner):
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }

    response = requests.delete(
        f"https://api.github.com/orgs/ClickHouse/actions/runners/{runner.id}",
        headers=headers,
    )
    response.raise_for_status()
    print(
        f"Response code deleting {runner.name} with id {runner.id} is {response.status_code}"
    )
    return response.status_code == 204


def main(github_secret_key, github_app_id, event):
    print("Got event", json.dumps(event, sort_keys=True, indent=4))
    to_kill_by_zone = how_many_instances_to_kill(event)
    instances_by_zone = get_candidates_to_be_killed(event)

    payload = {
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + (10 * 60),
        "iss": github_app_id,
    }

    encoded_jwt = jwt.encode(payload, github_secret_key, algorithm="RS256")
    installation_id = get_installation_id(encoded_jwt)
    access_token = get_access_token(encoded_jwt, installation_id)

    runners = list_runners(access_token)

    to_delete_runners = []
    instances_to_kill = []
    for zone in to_kill_by_zone:
        num_to_kill = to_kill_by_zone[zone]
        candidates = instances_by_zone[zone]
        if num_to_kill > len(candidates):
            raise Exception(
                f"Required to kill {num_to_kill}, but have only {len(candidates)} candidates in AV {zone}"
            )

        delete_for_av = []
        for candidate in candidates:
            if candidate not in set([runner.name for runner in runners]):
                print(
                    f"Candidate {candidate} was not in runners list, simply delete it"
                )
                instances_to_kill.append(candidate)

        for candidate in candidates:
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
                f"Checked all candidates for av {zone}, get to delete {len(delete_for_av)}, but still cannot get required {num_to_kill}"
            )
        to_delete_runners += delete_for_av

    print("Got instances to kill: ", ", ".join(instances_to_kill))
    print(
        "Going to delete runners:",
        ", ".join([runner.name for runner in to_delete_runners]),
    )
    for runner in to_delete_runners:
        if delete_runner(access_token, runner):
            print(
                f"Runner with name {runner.name} and id {runner.id} successfuly deleted from github"
            )
            instances_to_kill.append(runner.name)
        else:
            print(f"Cannot delete {runner.name} from github")

    ## push metrics
    # runners = list_runners(access_token)
    # push_metrics_to_cloudwatch(runners, 'RunnersMetrics')

    response = {"InstanceIDs": instances_to_kill}
    print(response)
    return response


def handler(event, context):
    private_key, app_id = get_key_and_app_from_aws()
    return main(private_key, app_id, event)


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

    main(private_key, args.app_id, sample_event)
