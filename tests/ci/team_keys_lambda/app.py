#!/usr/bin/env python3

import requests
import argparse
import json

from threading import Thread
from queue import Queue


def get_org_team_members(token: str, org: str, team_slug: str) -> tuple:
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get(
        f"https://api.github.com/orgs/{org}/teams/{team_slug}/members", headers=headers
    )
    response.raise_for_status()
    data = response.json()
    return tuple(m["login"] for m in data)


def get_members_keys(members: tuple) -> str:
    class Worker(Thread):
        def __init__(self, request_queue):
            Thread.__init__(self)
            self.queue = request_queue
            self.results = []

        def run(self):
            while True:
                m = self.queue.get()
                if m == "":
                    break
                response = requests.get(f"https://github.com/{m}.keys")
                self.results.append(f"# {m}\n{response.text}")
                self.queue.task_done()

    q = Queue()
    workers = []
    for m in members:
        q.put(m)
        # Create workers and add to the queue
        worker = Worker(q)
        worker.start()
        workers.append(worker)

    # Workers keep working till they receive an empty string
    for _ in workers:
        q.put("")

    # Join workers to wait till they finished
    for worker in workers:
        worker.join()

    responses = []
    for worker in workers:
        responses.extend(worker.results)
    return "".join(responses)


def get_token_from_aws() -> str:
    import boto3

    secret_name = "clickhouse_robot_token"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    data = json.loads(get_secret_value_response["SecretString"])
    return data["clickhouse_robot_token"]


def main(token: str, org: str, team_slug: str) -> str:
    members = get_org_team_members(token, org, team_slug)
    keys = get_members_keys(members)

    return keys


def handler(event, context):
    token = get_token_from_aws()
    result = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/html",
        },
        "body": main(token, "ClickHouse", "core"),
    }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get the public SSH keys for members of given org and team"
    )
    parser.add_argument("--token", required=True, help="Github PAT")
    parser.add_argument(
        "--organization", help="GitHub organization name", default="ClickHouse"
    )
    parser.add_argument("--team", help="GitHub team name", default="core")

    args = parser.parse_args()
    keys = main(args.token, args.organization, args.team)

    print(f"# Just shoing off the keys:\n{keys}")
