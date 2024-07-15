import json
import os

import boto3
import requests
from botocore.exceptions import ClientError

from pr_info import PRInfo
from ci_utils import Shell


class CIBuddy:
    _HEADERS = {"Content-Type": "application/json"}

    def __init__(self, dry_run=False):
        self.repo = os.getenv("GITHUB_REPOSITORY", "")
        self.dry_run = dry_run
        res = self._get_webhooks()
        self.test_channel = ""
        self.dev_ci_channel = ""
        if res:
            self.test_channel = json.loads(res)["test_channel"]
            self.dev_ci_channel = json.loads(res)["ci_channel"]
        self.job_name = os.getenv("CHECK_NAME", "unknown")
        pr_info = PRInfo()
        self.pr_number = pr_info.number
        self.head_ref = pr_info.head_ref
        self.commit_url = pr_info.commit_html_url

    @staticmethod
    def _get_webhooks():
        name = "ci_buddy_web_hooks"

        session = boto3.Session(region_name="us-east-1")  # Replace with your region
        ssm_client = session.client("ssm")
        json_string = None
        try:
            response = ssm_client.get_parameter(
                Name=name,
                WithDecryption=True,  # Set to True if the parameter is a SecureString
            )
            json_string = response["Parameter"]["Value"]
        except ClientError as e:
            print(f"An error occurred: {e}")

        return json_string

    def post(self, message, dry_run=None):
        if dry_run is None:
            dry_run = self.dry_run
        print(f"Posting slack message, dry_run [{dry_run}]")
        if dry_run:
            url = self.test_channel
        else:
            url = self.dev_ci_channel
        data = {"text": message}
        try:
            requests.post(url, headers=self._HEADERS, data=json.dumps(data), timeout=10)
        except Exception as e:
            print(f"ERROR: Failed to post message, ex {e}")

    def post_error(self, error_description, job_name="", with_instance_info=True):
        instance_id, instance_type = "unknown", "unknown"
        if with_instance_info:
            instance_id = Shell.run("ec2metadata --instance-id") or instance_id
            instance_type = Shell.run("ec2metadata --instance-type") or instance_type
        if not job_name:
            job_name = os.getenv("CHECK_NAME", "unknown")
        line_err = f":red_circle:    *Error: {error_description}*\n\n"
        line_ghr = f"   *Runner:*    `{instance_type}`, `{instance_id}`\n"
        line_job = f"   *Job:*          `{job_name}`\n"
        line_pr_ = f"   *PR:*           <https://github.com/{self.repo}/pull/{self.pr_number}|#{self.pr_number}>\n"
        line_br_ = f"   *Branch:*    `{self.head_ref}`, <{self.commit_url}|commit>\n"
        message = line_err
        message += line_job
        if with_instance_info:
            message += line_ghr
        if self.pr_number > 0:
            message += line_pr_
        else:
            message += line_br_
        self.post(message)


if __name__ == "__main__":
    # test
    buddy = CIBuddy(dry_run=True)
    buddy.post_error("Out of memory")
