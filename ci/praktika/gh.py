import json
import time
import traceback
from typing import Any

from ._environment import _Environment
from .result import Result
from .settings import Settings
from .utils import Shell


class GH:
    @classmethod
    def do_command_with_retries(cls, command):
        res = False
        retry_count = 0
        out, err = "", ""

        while retry_count < Settings.MAX_RETRIES_GH and not res:
            ret_code, out, err = Shell.get_res_stdout_stderr(command, verbose=True)
            res = ret_code == 0
            if not res and "Validation Failed" in err:
                print(f"ERROR: GH command validation error {[err]}")
                break
            if not res and "Bad credentials" in err:
                print("ERROR: GH credentials/auth failure")
                break
            if not res:
                retry_count += 1
                time.sleep(5)

        if not res:
            print(
                f"ERROR: Failed to execute gh command [{command}] out:[{out}] err:[{err}] after [{retry_count}] attempts"
            )
        return res

    @classmethod
    def post_pr_comment(
        cls, comment_body, or_update_comment_with_substring="", pr=None, repo=None
    ):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER
        if or_update_comment_with_substring:
            print(f"check comment [{comment_body}] created")
            cmd_check_created = f'gh api -H "Accept: application/vnd.github.v3+json" \
                "/repos/{repo}/issues/{pr}/comments" \
                --jq \'.[] | {{id: .id, body: .body}}\' | grep -F "{or_update_comment_with_substring}"'
            output = Shell.get_output(cmd_check_created)
            if output:
                comment_ids = []
                try:
                    comment_ids = [
                        json.loads(item.strip())["id"] for item in output.split("\n")
                    ]
                except Exception as ex:
                    print(f"Failed to retrieve PR comments with [{ex}]")
                for id in comment_ids:
                    cmd = f'gh api \
                       -X PATCH \
                          -H "Accept: application/vnd.github.v3+json" \
                             "/repos/{repo}/issues/comments/{id}" \
                             -f body=\'{comment_body}\''
                    print(f"Update existing comments [{id}]")
                    return cls.do_command_with_retries(cmd)

        cmd = f'gh pr comment {pr} --body "{comment_body}"'
        return cls.do_command_with_retries(cmd)

    @classmethod
    def get_pr_contributors(cls, pr=None, repo=None):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        cmd = f"gh pr view {pr} --repo {repo} --json commits --jq '[.commits[].authors[].login]'"
        contributors_str = Shell.get_output(cmd, verbose=True)
        res = []
        if contributors_str:
            try:
                res = json.loads(contributors_str)
            except Exception:
                print(
                    f"ERROR: Failed to fetch contributors list for PR [{pr}], repo [{repo}]"
                )
                traceback.print_exc()
        return res

    @classmethod
    def get_pr_labels(cls, pr=None, repo=None):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        cmd = f"gh pr view {pr} --repo {repo} --json labels --jq '.labels[].name'"
        output = Shell.get_output(cmd, verbose=True)
        res = []
        if output:
            res = output.splitlines()
        return list(set(res))

    @classmethod
    def get_pr_label_assigner(cls, label, pr=None, repo=None):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        cmd = f'gh api repos/{repo}/issues/{pr}/events --jq \'.[] | select(.event=="labeled" and .label.name=="{label}") | .actor.login\''
        return Shell.get_output(cmd, verbose=True)

    @classmethod
    def post_commit_status(cls, name, status, description, url):
        """
        Sets GH commit status
        :param name: commit status name
        :param status:
        :param description:
        :param url:
        :param repo:
        :return: True or False in case of error
        """
        status = cls.convert_to_gh_status(status)
        repo = _Environment.get().REPOSITORY
        command = (
            f"gh api -X POST -H 'Accept: application/vnd.github.v3+json' "
            f"/repos/{repo}/statuses/{_Environment.get().SHA} "
            f"-f state='{status}' -f target_url='{url}' "
            f"-f description='{description}' -f context='{name}'"
        )
        return cls.do_command_with_retries(command)

    @classmethod
    def post_foreign_commit_status(
        cls, name, status, description, url, repo, commit_sha
    ):
        """
        Sets GH commit status in foreign repo or commit
        :param name: commit status name
        :param status:
        :param description:
        :param url:
        :param repo: Foreign repo
        :param commit_sha: Commit in a foreign repo
        :return: True or False in case of error
        """
        status = cls.convert_to_gh_status(status)
        command = (
            f"gh api -X POST -H 'Accept: application/vnd.github.v3+json' "
            f"/repos/{repo}/statuses/{commit_sha} "
            f"-f state='{status}' -f target_url='{url}' "
            f"-f description='{description}' -f context='{name}'"
        )
        return cls.do_command_with_retries(command)

    @classmethod
    def convert_to_gh_status(cls, status):
        if status in (
            Result.Status.PENDING,
            Result.Status.SUCCESS,
            Result.Status.FAILED,
            Result.Status.ERROR,
        ):
            return status
        if status in Result.Status.RUNNING:
            return Result.Status.PENDING
        else:
            assert (
                False
            ), f"Invalid status [{status}] to be set as GH commit status.state"

    @classmethod
    def print_log_in_group(cls, group_name: str, lines: Any):
        if not isinstance(lines, (list, tuple, set)):
            lines = [lines]

        print(f"::group::{group_name}")
        for line in lines:
            print(line)
        print("::endgroup::")

    @classmethod
    def print_actions_debug_info(cls):
        cls.print_log_in_group("GITHUB_ENVS", Shell.get_output("env | grep ^GITHUB_"))
        cls.print_log_in_group(
            "GITHUB_EVENT", Shell.get_output("cat $GITHUB_EVENT_PATH")
        )


if __name__ == "__main__":
    # test
    GH.post_pr_comment(
        comment_body="foobar",
        or_update_comment_with_substring="CI",
        repo="ClickHouse/praktika",
        pr=15,
    )
