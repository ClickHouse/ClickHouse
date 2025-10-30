import dataclasses
import json
import os
import re
import tempfile
import time
import traceback
from typing import Dict, List, Optional, Union

from praktika._environment import _Environment
from praktika.info import Info
from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import Shell


class GH:
    @classmethod
    def get_changed_files(cls, strict=False) -> List[str]:
        info = Info()
        res = None

        if not info.is_local_run:
            repo_name = info.repo_name
            sha = info.sha
        else:
            repo_name = Shell.get_output(
                f"git config --get remote.origin.url | sed -E 's#(git@|https://)[^/:]+[:/](.*)\.git#\\2#'",
                strict=True,
            )
            sha = Shell.get_output(f"git rev-parse HEAD", strict=True)

        assert repo_name
        print(repo_name)

        for attempt in range(3):
            # store changed files
            if info.pr_number > 0:
                exit_code, changed_files_str, err = Shell.get_res_stdout_stderr(
                    f"gh pr view {info.pr_number} --repo {repo_name} --json files --jq '.files[].path'",
                )
                assert exit_code == 0, "Failed to retrieve changed files list"
            else:
                exit_code, changed_files_str, err = Shell.get_res_stdout_stderr(
                    f"gh api repos/{repo_name}/commits/{sha} | jq -r '.files[].filename'",
                )

            if exit_code == 0:
                res = changed_files_str.split("\n") if changed_files_str else []
                break
            else:
                print(
                    f"Failed to get changed files, attempt [{attempt+1}], exit code [{exit_code}], error [{err}]"
                )
                if exit_code > 1:
                    # assume that exit code == 1 is retryable - Fix if not true
                    # exit_code 1 for this type of errors:  WARNING: stderr: GraphQL: Something went wrong while executing your query on 2025-08-05T15:33:56Z. Please include `E746:1CAA99:44F9F67:8B9B520:68922464` when reporting this issue.
                    print("error is not retryable - break")
                    break
                time.sleep(1)

        if res is None and strict:
            raise RuntimeError("Failed to get changed files")

        return res

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
            if not res and "Resource not accessible" in err:
                print("ERROR: GH permissions failure")
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
    def post_updateable_comment(
        cls,
        comment_tags_and_bodies: Dict[str, str],
        pr=None,
        repo=None,
        only_update=False,
    ):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        TAG_COMMENT_START = "<!-- CI automatic comment start :{TAG}: -->"
        TAG_COMMENT_END = "<!-- CI automatic comment end :{TAG}: -->"
        cmd_check_created = f'gh api -H "Accept: application/vnd.github.v3+json" \
            "/repos/{repo}/issues/{pr}/comments" \
            --jq \'[.[] | {{id: .id, body: .body}}]\' --paginate'
        output = Shell.get_output(cmd_check_created, verbose=True)

        comments = json.loads(output)

        comment_to_update = None
        id_to_update = None
        for tag, body in comment_tags_and_bodies.items():
            start_tag = TAG_COMMENT_START.format(TAG=tag)
            end_tag = TAG_COMMENT_END.format(TAG=tag)
            if not comment_to_update:
                for comment in comments:
                    if start_tag in comment["body"] and end_tag in comment["body"]:
                        comment_to_update = comment
                        id_to_update = comment["id"]
                        print(f"Found comment to update [{id_to_update}]")
                        break
            else:
                if (
                    start_tag not in comment_to_update["body"]
                    or end_tag not in comment_to_update["body"]
                ):
                    print(
                        f"WARNING: Comment [{id_to_update}] has no tag [{tag}] - will append"
                    )

        body = "" if not comment_to_update else comment_to_update["body"]
        for tag, tag_body in comment_tags_and_bodies.items():
            start_tag = TAG_COMMENT_START.format(TAG=tag)
            end_tag = TAG_COMMENT_END.format(TAG=tag)
            if not comment_to_update:
                body += f"{start_tag}\n{tag_body}\n{end_tag}\n"
            else:
                if start_tag in body and end_tag in body:
                    rex = re.compile(
                        f"{re.escape(start_tag)}.*{re.escape(end_tag)}", re.DOTALL
                    )
                    body, _ = rex.subn(f"{start_tag}\n{tag_body}\n{end_tag}", body)
                    print(
                        f"Updated existing comment [{id_to_update}] tag [{tag}] with [{tag_body}], new [{body}]"
                    )
                else:
                    body = body.removesuffix("\n") + "\n"
                    body += f"{start_tag}\n{tag_body}\n{end_tag}\n"
                    print(
                        f"Appended existing comment [{id_to_update}] tag [{tag}] with [{tag_body}], new [{body}]"
                    )

        # Create temp file for body to avoid shell escaping issues
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt", encoding="utf-8"
        ) as temp_file:
            temp_file.write(body)
            temp_file_path = temp_file.name

        res = None
        if id_to_update:
            cmd = f'gh api -X PATCH \
                    -H "Accept: application/vnd.github.v3+json" \
                    "/repos/{repo}/issues/comments/{id_to_update}" \
                    -F body=@{temp_file_path}'
            print(f"Update existing comments [{id_to_update}]")
            res = cls.do_command_with_retries(cmd)
        else:
            if not only_update:
                cmd = f"gh pr comment {pr} --body-file {temp_file_path}"
                print(f"Create new comment")
                res = cls.do_command_with_retries(cmd)
            else:
                print(
                    f"WARNING: comment to update not found, tags [{[k for k in comment_tags_and_bodies.keys()]}]"
                )

        # Clean up temp file
        os.unlink(temp_file_path)

        return res

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
    def get_pr_title_body_labels(cls, pr=None, repo=None):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        cmd = f"gh pr view {pr} --json title,body,labels --repo {repo}"
        output = Shell.get_output(cmd, verbose=True)
        try:
            pr_data = json.loads(output)
            title = pr_data["title"]
            body = pr_data["body"]
            labels = [l["name"] for l in pr_data["labels"]]
        except Exception:
            print("ERROR: Failed to get PR data")
            traceback.print_exc()
            Info().store_traceback()
            return "", "", []
        return title, body, labels

    @classmethod
    def get_pr_label_assigner(cls, label, pr=None, repo=None):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        cmd = f'gh api repos/{repo}/issues/{pr}/events --jq \'.[] | select(.event=="labeled" and .label.name=="{label}") | .actor.login\''
        return Shell.get_output(cmd, verbose=True)

    @classmethod
    def get_pr_diff(cls, pr=None, repo=None):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        cmd = f"gh pr diff {pr} --repo {repo}"
        return Shell.get_output(cmd, verbose=True)

    @classmethod
    def update_pr_body(cls, new_body=None, body_file=None, pr=None, repo=None):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        assert new_body or body_file, "Either new_body or body_file must be provided"
        assert not (
            new_body and body_file
        ), "Cannot provide both new_body and body_file"

        if body_file:
            # Use file for body to avoid shell escaping issues
            cmd = f'gh api -X PATCH \
                -H "Accept: application/vnd.github.v3+json" \
                "/repos/{repo}/pulls/{pr}" \
                -F body=@{body_file}'
        else:
            # Use inline body (original behavior)
            escaped_body = new_body.replace("'", "'\"'\"'")
            cmd = f'gh api -X PATCH \
                -H "Accept: application/vnd.github.v3+json" \
                "/repos/{repo}/pulls/{pr}" \
                -f body=\'{escaped_body}\''

        return cls.do_command_with_retries(cmd)

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
        description_max_size = 80  # GH limits to 140, but 80 is reasonable
        description = description[:description_max_size].replace(
            "'", "'\"'\"'"
        )  # escape single quote
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
        description_max_size = 80  # GH limits to 140, but 80 is reasonable
        description = description[:description_max_size].replace(
            "'", "'\"'\"'"
        )  # escape single quote
        status = cls.convert_to_gh_status(status)
        command = (
            f"gh api -X POST -H 'Accept: application/vnd.github.v3+json' "
            f"/repos/{repo}/statuses/{commit_sha} "
            f"-f state='{status}' -f target_url='{url}' "
            f"-f description='{description}' -f context='{name}'"
        )
        return cls.do_command_with_retries(command)

    @classmethod
    def merge_pr(cls, pr=None, repo=None, squash=False, keep_branch=False):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        extra_args = ""
        if not keep_branch:
            extra_args += " --delete-branch"
        if squash:
            extra_args += " --squash"
        else:
            extra_args += " --merge"

        cmd = f"gh pr merge {pr} --repo {repo} {extra_args}"
        return cls.do_command_with_retries(cmd)

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
    def print_log_in_group(cls, group_name: str, lines: Union[str, List[str]]):
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

    @dataclasses.dataclass
    class ResultSummaryForGH:
        name: str
        status: Result.Status
        sha: str = ""
        start_time: Optional[float] = None
        duration: Optional[float] = None
        failed_results: List["ResultSummaryForGH"] = dataclasses.field(
            default_factory=list
        )
        info: str = ""
        comment: str = ""

        @classmethod
        def from_result(cls, result: Result):
            MAX_TEST_CASES_PER_JOB = 10
            MAX_JOBS_PER_SUMMARY = 10

            def flatten_results(results):
                for r in results:
                    if not r.results:
                        yield r
                    else:
                        yield from flatten_results(r.results)

            def extract_hlabels_info(res: Result) -> str:
                try:
                    hlabels = (
                        res.ext.get("hlabels", [])
                        if hasattr(res, "ext") and isinstance(res.ext, dict)
                        else []
                    )
                    links = []
                    for item in hlabels:
                        text = None
                        href = None
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            text, href = item[0], item[1]
                        if text and href:
                            links.append(f"[{text}]({href})")
                    return ", ".join(links)
                except Exception:
                    return ""

            info = Info()
            summary = cls(
                name=result.name,
                status=result.status,
                sha=info.sha,
                start_time=result.start_time,
                duration=result.duration,
                failed_results=[],
                info=extract_hlabels_info(result),
                comment="",
            )
            for sub_result in result.results:
                if sub_result.is_completed() and not sub_result.is_ok():
                    failed_result = cls(
                        name=sub_result.name,
                        status=sub_result.status,
                        info=extract_hlabels_info(sub_result),
                        comment="",
                    )
                    failed_result.failed_results = [
                        cls(
                            name=r.name,
                            status=r.status,
                            info=extract_hlabels_info(r),
                            comment="",
                        )
                        for r in flatten_results(sub_result.results)
                        if r.is_completed() and not r.is_ok()
                    ]
                    if len(failed_result.failed_results) > MAX_TEST_CASES_PER_JOB:
                        remaining = (
                            len(failed_result.failed_results) - MAX_TEST_CASES_PER_JOB
                        )
                        note = f"{remaining} more test cases not shown"
                        failed_result.failed_results = failed_result.failed_results[
                            :MAX_TEST_CASES_PER_JOB
                        ]
                        failed_result.failed_results.append(cls(name=note, status=""))
                    summary.failed_results.append(failed_result)
            if len(summary.failed_results) > MAX_JOBS_PER_SUMMARY:
                remaining = len(summary.failed_results) - MAX_JOBS_PER_SUMMARY
                summary.failed_results = summary.failed_results[:MAX_JOBS_PER_SUMMARY]
                print(f"NOTE: {remaining} more jobs not shown in PR comment")
            return summary

        def to_markdown(self):
            if self.status == Result.Status.SUCCESS:
                symbol = "✅"  # Green check mark
            elif self.status == Result.Status.FAILED:
                symbol = "❌"  # Red cross mark
            else:
                symbol = "⏳"  # Hourglass (in progress)

            body = f"**Summary:** {symbol}\n"

            if self.failed_results:
                if len(self.failed_results) > 15:
                    body += (
                        f"    *15 failures out of {len(self.failed_results)} shown*:\n"
                    )
                    self.failed_results = self.failed_results[:15]
                body += "|job_name|test_name|status|info|comment|\n"
                body += "|:--|:--|:-:|:--|:--|\n"
                info = Info()
                for failed_result in self.failed_results:
                    job_report_url = info.get_specific_report_url(
                        info.pr_number,
                        info.git_branch,
                        info.sha,
                        failed_result.name,
                        info.workflow_name,
                    )
                    body += "|[{}]({})|{}|{}|{}|{}|\n".format(
                        failed_result.name,
                        job_report_url,
                        "",
                        failed_result.status,
                        failed_result.info or "",
                        failed_result.comment or "",
                    )
                    if failed_result.failed_results:
                        for sub_failed_result in failed_result.failed_results:
                            body += "|{}|{}|{}|{}|{}|\n".format(
                                "",
                                sub_failed_result.name,
                                sub_failed_result.status,
                                sub_failed_result.info or "",
                                sub_failed_result.comment or "",
                            )
            return body


if __name__ == "__main__":
    # test
    GH.post_updateable_comment(
        comment_tags_and_bodies={
            "test": "foobar4",
            "test3": "foobar33",
        },
        pr=81471,
        repo="ClickHouse/ClickHouse",
    )
