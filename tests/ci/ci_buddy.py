import argparse
import json
import os
from typing import Union, Dict

import boto3
import requests
from botocore.exceptions import ClientError

from pr_info import PRInfo
from ci_utils import Shell, GHActions


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
        self.sha = pr_info.sha[:10]

    def check_workflow(self):
        GHActions.print_workflow_results()
        res = GHActions.get_workflow_job_result(GHActions.ActionsNames.RunConfig)
        if res != GHActions.ActionStatuses.SUCCESS:
            self.post_job_error("Workflow Configuration Failed", critical=True)

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

    def _post_formatted(
        self, title: str, body: Union[Dict, str], with_wf_link: bool
    ) -> None:
        message = title
        if isinstance(body, dict):
            for name, value in body.items():
                if "commit_sha" in name:
                    value = (
                        f"<https://github.com/{self.repo}/commit/{value}|{value[:8]}>"
                    )
                message += f"      *{name}*:    {value}\n"
        else:
            message += body + "\n"
        run_id = os.getenv("GITHUB_RUN_ID", "")
        if with_wf_link and run_id:
            message += f"      *workflow*: <https://github.com/{self.repo}/actions/runs/{run_id}|{run_id}>\n"
        self.post(message)

    def post_info(
        self, title: str, body: Union[Dict, str], with_wf_link: bool = True
    ) -> None:
        title_extended = f":white_circle:    *{title}*\n\n"
        self._post_formatted(title_extended, body, with_wf_link)

    def post_done(
        self, title: str, body: Union[Dict, str], with_wf_link: bool = True
    ) -> None:
        title_extended = f":white_check_mark:    *{title}*\n\n"
        self._post_formatted(title_extended, body, with_wf_link)

    def post_warning(
        self, title: str, body: Union[Dict, str], with_wf_link: bool = True
    ) -> None:
        title_extended = f":warning:    *{title}*\n\n"
        self._post_formatted(title_extended, body, with_wf_link)

    def post_critical(
        self, title: str, body: Union[Dict, str], with_wf_link: bool = True
    ) -> None:
        title_extended = f":black_circle:    *{title}*\n\n"
        self._post_formatted(title_extended, body, with_wf_link)

    def post_job_error(
        self,
        error_description: str,
        job_name: str = "",
        with_instance_info: bool = True,
        with_wf_link: bool = True,
        critical: bool = False,
    ) -> None:
        instance_id, instance_type = "unknown", "unknown"
        if with_instance_info:
            instance_id = Shell.run("ec2metadata --instance-id") or instance_id
            instance_type = Shell.run("ec2metadata --instance-type") or instance_type
        if not job_name:
            job_name = os.getenv("CHECK_NAME", "unknown")
        sign = ":red_circle:" if not critical else ":black_circle:"
        line_err = f"{sign}    *Error: {error_description}*\n\n"
        line_ghr = f"   *Runner:*    `{instance_type}`, `{instance_id}`\n"
        line_job = f"   *Job:*          `{job_name}`\n"
        line_pr_ = f"   *PR:*           <https://github.com/{self.repo}/pull/{self.pr_number}|#{self.pr_number}>, <{self.commit_url}|{self.sha}>\n"
        line_br_ = (
            f"   *Branch:*    `{self.head_ref}`, <{self.commit_url}|{self.sha}>\n"
        )
        message = line_err
        message += line_job
        if with_instance_info:
            message += line_ghr
        if self.pr_number > 0:
            message += line_pr_
        else:
            message += line_br_
        run_id = os.getenv("GITHUB_RUN_ID", "")
        if with_wf_link and run_id:
            message += f"      *workflow*: <https://github.com/{self.repo}/actions/runs/{run_id}|{run_id}>\n"
        self.post(message)


def parse_args():
    parser = argparse.ArgumentParser("CI Buddy bot notifies about CI events")
    parser.add_argument(
        "--check-wf-status",
        action="store_true",
        help="Checks workflow status",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="for test and debug",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="dry run mode",
    )
    return parser.parse_args(), parser


if __name__ == "__main__":
    args, parser = parse_args()

    if args.test:
        CIBuddy(dry_run=True).post_job_error("TEst")
    elif args.check_wf_status:
        CIBuddy(dry_run=args.dry_run).check_workflow()
