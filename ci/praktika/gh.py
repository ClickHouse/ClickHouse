import json
import time

from praktika._environment import _Environment
from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import Shell


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
                print("ERROR: GH command validation error")
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
        cls, comment_body, or_update_comment_with_substring, repo=None, pr=None
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
    def post_commit_status(cls, name, status, description, url):
        status = cls.convert_to_gh_status(status)
        command = (
            f"gh api -X POST -H 'Accept: application/vnd.github.v3+json' "
            f"/repos/{_Environment.get().REPOSITORY}/statuses/{_Environment.get().SHA} "
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


if __name__ == "__main__":
    # test
    GH.post_pr_comment(
        comment_body="foobar",
        or_update_comment_with_substring="CI",
        repo="ClickHouse/praktika",
        pr=15,
    )
