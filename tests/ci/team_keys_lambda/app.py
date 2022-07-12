#!/usr/bin/env python3

import argparse
import json

from datetime import datetime
from queue import Queue
from threading import Thread

import requests  # type: ignore
import boto3  # type: ignore


class Keys(set):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.updated_at = 0

    def update_now(self):
        self.updated_at = datetime.now().timestamp()


keys = Keys()


class Worker(Thread):
    def __init__(self, request_queue):
        Thread.__init__(self)
        self.queue = request_queue
        self.results = set()

    def run(self):
        while True:
            m = self.queue.get()
            if m == "":
                break
            response = requests.get(f"https://github.com/{m}.keys")
            self.results.add(f"# {m}\n{response.text}\n")
            self.queue.task_done()


def get_org_team_members(token: str, org: str, team_slug: str) -> set:
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get(
        f"https://api.github.com/orgs/{org}/teams/{team_slug}/members", headers=headers
    )
    response.raise_for_status()
    data = response.json()
    return set(m["login"] for m in data)


def get_cached_members_keys(members: set) -> Keys:
    if (datetime.now().timestamp() - 3600) <= keys.updated_at:
        return keys

    q = Queue()  # type: Queue
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

    keys.clear()
    for worker in workers:
        keys.update(worker.results)
    keys.update_now()
    return keys


def get_token_from_aws() -> str:
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
    keys = get_cached_members_keys(members)

    return "".join(sorted(keys))


def handler(event, context):
    _ = context
    _ = event
    if keys.updated_at < (datetime.now().timestamp() - 3600):
        token = get_token_from_aws()
        body = main(token, "ClickHouse", "core")
    else:
        body = "".join(sorted(keys))

    result = {
        "statusCode": 200,
        "headers": {
            "Content-Type": "text/html",
        },
        "body": body,
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
    output = main(args.token, args.organization, args.team)

    print(f"# Just shoing off the keys:\n{output}")
