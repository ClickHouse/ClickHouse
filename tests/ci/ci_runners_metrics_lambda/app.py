#!/usr/bin/env python3
"""
Lambda function to:
    - calculate number of running runners
    - cleaning dead runners from GitHub
    - terminating stale lost runners in EC2
"""

import argparse
import sys
from typing import Dict

import boto3  # type: ignore
from lambda_shared import RUNNER_TYPE_LABELS, RunnerDescriptions, list_runners
from lambda_shared.token import (
    get_access_token_by_key_app,
    get_cached_access_token,
    get_key_and_app_from_aws,
)

UNIVERSAL_LABEL = "universal"


def handler(event, context):
    _ = event
    _ = context
    main(get_cached_access_token(), True)


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


def main(
    access_token: str,
    push_to_cloudwatch: bool,
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

    main(token, args.push_to_cloudwatch)
