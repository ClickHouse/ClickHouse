import dataclasses
import json
import os
import re
import shlex
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

    @dataclasses.dataclass
    class GHIssue:
        title: str
        body: str
        labels: List[str]
        author: str
        url: str
        updated_at: str
        created_at: str
        number: int
        is_closed: bool = False

        @property
        def html_url(self):
            """Alias for url field for compatibility"""
            return self.url

        @property
        def state(self):
            """Backwards compatibility property for state field"""
            return "closed" if self.is_closed else "open"

        @classmethod
        def from_gh_json(cls, json_data):
            state_str = json_data.get("state", "open").lower()
            return cls(
                title=json_data["title"],
                body=json_data["body"],
                labels=[label["name"] for label in json_data["labels"]],
                author=json_data["author"]["login"],
                url=json_data["url"],
                updated_at=json_data["updatedAt"],
                created_at=json_data["createdAt"],
                number=json_data["number"],
                is_closed=(state_str == "closed"),
            )

    @dataclasses.dataclass
    class CommitStatus:
        state: str
        description: str
        url: str
        context: str

    @classmethod
    def get_changed_files(cls, strict=False) -> List[str]:
        info = Info()
        res = None

        if not info.is_local_run:
            repo_name = info.repo_name
            sha = info.sha
        else:
            repo_name = Shell.get_output(
                rf"git config --get remote.origin.url | sed -E 's#(git@|https://)[^/:]+[:/](.*)\.git#\\2#'",
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
    def do_command_with_retries(cls, command, verbose=False):
        res = False
        retry_count = 0
        out, err = "", ""

        while retry_count < Settings.MAX_RETRIES_GH and not res:
            ret_code, out, err = Shell.get_res_stdout_stderr(command, verbose=verbose)
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
                delay = min(2 ** (retry_count + 1), 60)
                time.sleep(delay)

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

        temp_file_path = None
        try:
            if or_update_comment_with_substring:
                print(f"check comment [{comment_body}] created")
                safe_substr = shlex.quote(or_update_comment_with_substring)
                cmd_check_created = (
                    f'gh api -H "Accept: application/vnd.github.v3+json" '
                    f'"/repos/{repo}/issues/{pr}/comments" '
                    f"--jq '.[] | {{id: .id, body: .body}}' | grep -F {safe_substr}"
                )
                output = Shell.get_output(cmd_check_created)
                if output:
                    comment_ids = []
                    try:
                        comment_ids = [
                            json.loads(item.strip())["id"]
                            for item in output.split("\n")
                            if item.strip()
                        ]
                    except Exception as ex:
                        print(f"Failed to retrieve PR comments with [{ex}]")
                    if comment_ids:
                        with tempfile.NamedTemporaryFile(
                            mode="w", delete=False, suffix=".txt", encoding="utf-8"
                        ) as temp_file:
                            temp_file.write(comment_body)
                            temp_file_path = temp_file.name
                        for id in comment_ids:
                            cmd = f'gh api \
                               -X PATCH \
                                  -H "Accept: application/vnd.github.v3+json" \
                                     "/repos/{repo}/issues/comments/{id}" \
                                     -F body=@{temp_file_path}'
                            print(f"Update existing comments [{id}]")
                            return cls.do_command_with_retries(cmd)

            # default: create a new comment using a temporary file to avoid shell escaping/injection
            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".txt", encoding="utf-8"
            ) as temp_file:
                temp_file.write(comment_body)
                temp_file_path = temp_file.name

            cmd = f"gh pr comment {pr} --body-file {temp_file_path}"
            return cls.do_command_with_retries(cmd)
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                except Exception:
                    pass

    @classmethod
    def post_updateable_comment(
        cls,
        comment_tags_and_bodies: Dict[str, str],
        pr=None,
        repo=None,
        only_update=False,
        verbose=True,
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
        output = Shell.get_output(cmd_check_created, verbose=verbose)

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
                        if verbose:
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
                    if verbose:
                        print(
                            f"Updated existing comment [{id_to_update}] tag [{tag}] with [{tag_body}], new [{body}]"
                        )
                else:
                    body = body.removesuffix("\n") + "\n"
                    body += f"{start_tag}\n{tag_body}\n{end_tag}\n"
                    if verbose:
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
            if verbose:
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
    def post_commit_status(cls, name, status, description, url, sha="", repo=""):
        """
        Sets GH commit status
        :param name: commit status name
        :param status:
        :param description:
        :param url:
        :param sha: commit SHA (defaults to current environment SHA)
        :param repo: repository in format owner/repo (defaults to current environment repo)
        :return: True or False in case of error
        """
        description_max_size = 80  # GH limits to 140, but 80 is reasonable
        description = description[:description_max_size]
        status = cls.convert_to_gh_status(status)
        repo = repo or _Environment.get().REPOSITORY
        sha = sha or _Environment.get().SHA

        safe_state = shlex.quote(str(status))
        safe_target = shlex.quote(str(url))
        safe_description = shlex.quote(str(description))
        safe_context = shlex.quote(str(name))

        command = (
            f"gh api -X POST -H 'Accept: application/vnd.github.v3+json' "
            f"/repos/{repo}/statuses/{sha} "
            f"-f state={safe_state} -f target_url={safe_target} "
            f"-f description={safe_description} -f context={safe_context}"
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
        description = description[:description_max_size]
        status = cls.convert_to_gh_status(status)

        safe_state = shlex.quote(str(status))
        safe_target = shlex.quote(str(url))
        safe_description = shlex.quote(str(description))
        safe_context = shlex.quote(str(name))

        command = (
            f"gh api -X POST -H 'Accept: application/vnd.github.v3+json' "
            f"/repos/{repo}/statuses/{commit_sha} "
            f"-f state={safe_state} -f target_url={safe_target} "
            f"-f description={safe_description} -f context={safe_context}"
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

    @staticmethod
    def pr_has_conflicts(pr=None, repo=None, verbose=False):
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        cmd = f"gh pr view {pr} --repo {repo} --json mergeable --jq .mergeable"
        output = Shell.get_output(cmd, verbose=verbose)
        return output == "CONFLICTING"

    @classmethod
    def create_issue(
        cls,
        title,
        body,
        labels: List[str] = None,
        repo="",
        verbose=False,
        no_strict=False,
    ) -> Optional[str]:
        """
        Create a GitHub issue and return its URL.

        Returns:
            Issue URL string on success, None on failure
        """
        if not repo:
            repo = _Environment.get().REPOSITORY
        if labels is None:
            labels = []

        # GitHub API limit for issue body is 65536 characters
        max_body_length = 65536
        if len(body) > max_body_length:
            truncation_note = "\n\n... (truncated due to GitHub body size limit)"
            body = body[: max_body_length - len(truncation_note)] + truncation_note

        temp_file_path = None
        try:
            # Create temp file for body to avoid shell escaping issues
            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".txt", encoding="utf-8"
            ) as temp_file:
                temp_file.write(body)
                temp_file_path = temp_file.name

            safe_repo = shlex.quote(repo)
            safe_title = shlex.quote(title)
            safe_body_file = shlex.quote(temp_file_path)
            label_cmd = "".join(f" --label {shlex.quote(label)}" for label in labels)
            cmd = (
                f"gh issue create --repo {safe_repo} --title {safe_title} "
                f"--body-file {safe_body_file}{label_cmd}"
            )
            issue_url = Shell.get_output_or_raise(cmd, verbose=verbose)
            assert issue_url, "Failed to create issue"
            return issue_url
        except Exception:
            if verbose:
                print("ERROR: Failed to create issue")
                traceback.print_exc()
            if not no_strict:
                assert False, "Failed to create issue"
        finally:
            if temp_file_path and os.path.exists(temp_file_path):
                os.unlink(temp_file_path)
        return None

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
        elif status in Result.Status.DROPPED:
            return Result.Status.ERROR
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
        def from_result(cls, result: Result, sha=""):
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
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            text, href = item[0], item[1]
                        if text and href:
                            links.append(f"[{text}]({href})")
                    return ", ".join(links)
                except Exception:
                    return ""

            summary = cls(
                name=result.name,
                status=result.status,
                sha=sha or Info().sha,
                start_time=result.start_time,
                duration=result.duration,
                failed_results=[],
                info=extract_hlabels_info(result),
                comment=result.ext.get("comment", ""),
            )

            # Filter and sort failed/error subresults by priority
            # Priority: FAILED (0) > ERROR (1) > others (2)
            def get_status_priority(r):
                if r.status == Result.Status.FAILED:
                    return 0
                elif r.status == Result.Status.ERROR:
                    return 1
                else:
                    return 2

            subresults = [
                r for r in result.results if (r.is_completed() and not r.is_ok())
            ]
            subresults = sorted(subresults, key=get_status_priority)

            for sub_result in subresults:
                failed_result = cls(
                    name=sub_result.name,
                    status=sub_result.status,
                    info=extract_hlabels_info(sub_result),
                    comment=sub_result.ext.get("comment", ""),
                )
                failed_result.failed_results = [
                    cls(
                        name=r.name,
                        status=r.status,
                        info=extract_hlabels_info(r),
                        comment=r.ext.get("comment", ""),
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

        def to_markdown(self, pr_number=0, sha="", workflow_name="", branch=""):
            def escape_pipes(text):
                """Escape pipe characters for markdown tables"""
                return str(text).replace("|", "\\|")

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
                if not ((pr_number or branch) and sha and workflow_name):
                    info = Info()
                    pr_number = info.pr_number
                    sha = info.sha
                    workflow_name = info.workflow_name
                    branch = info.git_branch
                for failed_result in self.failed_results:
                    job_report_url = Info.get_specific_report_url_static(
                        pr_number,
                        branch,
                        sha,
                        failed_result.name,
                        workflow_name,
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
                                # Logical erros might have | that break comment formatting
                                escape_pipes(sub_failed_result.name),
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
