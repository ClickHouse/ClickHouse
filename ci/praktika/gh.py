import dataclasses
import html
import json
import os
import re
import shutil
import shlex
import tempfile
import time
import traceback
from pathlib import Path, PurePosixPath
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
            repo_url = Shell.get_output("git config --get remote.origin.url", strict=True)
            repo_name = cls._repo_name_from_git_remote_url(repo_url)
            if not repo_name:
                raise RuntimeError(
                    f"Failed to extract repository name from remote URL [{repo_url}]"
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

    @staticmethod
    def _repo_name_from_git_remote_url(repo_url: str) -> str:
        match = re.match(
            r"^(?:https?://[^/]+/|git@[^:]+:|ssh://git@[^/]+/)([^/\s]+/[^/\s]+?)(?:\.git)?/?$",
            repo_url,
        )
        return match.group(1) if match else ""

    @staticmethod
    def _normalize_gh_pages_destination(destination_dir: str) -> str:
        destination_dir = (destination_dir or "").strip().strip("/")
        if not destination_dir:
            return ""

        parts = PurePosixPath(destination_dir).parts
        if any(part in ("", ".", "..") for part in parts):
            raise ValueError(f"Invalid GitHub Pages destination [{destination_dir}]")

        return PurePosixPath(*parts).as_posix()

    @staticmethod
    def _git_env_with_token(temp_root: Path, token: str) -> Dict[str, str]:
        if not token:
            raise RuntimeError("GitHub Pages publish requires a GitHub token")

        token_file = temp_root / "github-token"
        askpass_file = temp_root / "git-askpass.sh"
        token_file.write_text(token, encoding="utf-8")
        os.chmod(token_file, 0o600)
        askpass_file.write_text(
            "#!/bin/sh\n"
            'case "$1" in\n'
            "  *Username*) printf '%s\\n' 'x-access-token' ;;\n"
            f"  *) cat {shlex.quote(str(token_file))} ;;\n"
            "esac\n",
            encoding="utf-8",
        )
        os.chmod(askpass_file, 0o700)

        env = os.environ.copy()
        env["GIT_ASKPASS"] = str(askpass_file)
        env["GIT_TERMINAL_PROMPT"] = "0"
        return env

    @classmethod
    def gh_pages_url(cls, repo="", destination_dir="") -> str:
        repo = repo or _Environment.get().REPOSITORY
        if not repo:
            repo_url = Shell.get_output(
                "git config --get remote.origin.url", strict=True
            )
            repo = cls._repo_name_from_git_remote_url(repo_url)
        if not repo:
            raise RuntimeError("Failed to resolve repository name for GitHub Pages")

        owner, name = repo.split("/", 1)
        destination_dir = cls._normalize_gh_pages_destination(destination_dir)
        suffix = f"/{destination_dir}" if destination_dir else ""
        return f"https://{owner.lower()}.github.io/{name}{suffix}/"

    @classmethod
    def _write_gh_pages_index(cls, worktree: Path):
        entries = []
        for item in sorted(worktree.iterdir(), key=lambda path: path.name.lower()):
            if item.name in {".git", ".nojekyll", "index.html"}:
                continue
            if item.name.startswith("."):
                continue
            href = f"{item.name}/" if item.is_dir() else item.name
            label = f"{item.name}/" if item.is_dir() else item.name
            entries.append((href, label))

        list_items = "\n".join(
            f'  <li><a href="{html.escape(href, quote=True)}">'
            f"{html.escape(label)}</a></li>"
            for href, label in entries
        )
        worktree.joinpath("index.html").write_text(
            "\n".join(
                [
                    "<!doctype html>",
                    '<html lang="en">',
                    "<head>",
                    '<meta charset="utf-8">',
                    '<meta name="viewport" content="width=device-width, initial-scale=1">',
                    "<title>Pages index</title>",
                    "</head>",
                    "<body>",
                    "<h1>Pages index</h1>",
                    "<ul>",
                    list_items,
                    "</ul>",
                    "</body>",
                    "</html>",
                    "",
                ]
            ),
            encoding="utf-8",
        )

    @classmethod
    def publish_gh_pages(
        cls,
        source_dir: str,
        destination_dir: str = "",
        branch: str = "gh-pages",
        commit_message: str = "",
        repo: str = "",
        no_jekyll: bool = True,
        github_token: str = "",
        git_user_name: str = "praktika[bot]",
        git_user_email: str = "praktika[bot]@users.noreply.github.com",
        verbose: bool = True,
        clean_destination: bool = True,
        update_root_index: bool = True,
    ) -> str:
        """Publish a local directory to a path on the repository's Pages branch.

        Publishes through Praktika's GitHub App token. Praktika jobs that call
        this should use ``Job.Config.enable_gh_auth=True`` so runner-side auth
        setup and token-minting permissions are available.
        """

        source = Path(source_dir).resolve()
        if not source.is_dir():
            raise FileNotFoundError(f"GitHub Pages source dir not found [{source}]")

        destination_dir = cls._normalize_gh_pages_destination(destination_dir)
        repo = repo or _Environment.get().REPOSITORY
        if not repo:
            repo_url = Shell.get_output(
                "git config --get remote.origin.url", strict=True
            )
            repo = cls._repo_name_from_git_remote_url(repo_url)
        if not repo:
            raise RuntimeError("Failed to resolve repository name for GitHub Pages")

        temp_root = tempfile.mkdtemp(prefix="praktika-gh-pages-")
        worktree = Path(temp_root) / "worktree"
        remote_name = f"praktika-gh-pages-{os.getpid()}"
        safe_branch = shlex.quote(branch)
        safe_remote_name = shlex.quote(remote_name)
        git_env = None
        remote_added = False
        try:
            if not github_token:
                from praktika.gh_auth import GHAuth

                github_token = GHAuth.get_installation_token(
                    required_permissions={"contents": "write"}
                )
            git_env = cls._git_env_with_token(Path(temp_root), github_token)
            remote_url = f"https://github.com/{repo}.git"
            Shell.check(
                f"git remote add {safe_remote_name} {shlex.quote(remote_url)}",
                strict=True,
                verbose=verbose,
            )
            remote_added = True
            branch_exists = (
                Shell.run(
                    f"git ls-remote --exit-code --heads {safe_remote_name} {safe_branch}",
                    verbose=verbose,
                    env=git_env,
                )
                == 0
            )
            if branch_exists:
                remote_branch_ref = shlex.quote(f"{remote_name}/{branch}")
                fetch_refspec = shlex.quote(
                    f"+refs/heads/{branch}:refs/remotes/{remote_name}/{branch}"
                )
                Shell.check(
                    f"git fetch --depth=1 {safe_remote_name} {fetch_refspec}",
                    strict=True,
                    verbose=verbose,
                    env=git_env,
                )
                Shell.check(
                    "git worktree add --detach "
                    f"{shlex.quote(str(worktree))} {remote_branch_ref}",
                    strict=True,
                    verbose=verbose,
                )
            else:
                Shell.check(
                    f"git worktree add --detach {shlex.quote(str(worktree))}",
                    strict=True,
                    verbose=verbose,
                )
                Shell.check(
                    f"git -C {shlex.quote(str(worktree))} checkout --orphan {safe_branch}",
                    strict=True,
                    verbose=verbose,
                )
                Shell.check(
                    f"git -C {shlex.quote(str(worktree))} rm -rf --ignore-unmatch .",
                    strict=True,
                    verbose=verbose,
                )

            target = worktree / destination_dir if destination_dir else worktree
            if target.exists() and clean_destination:
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
            elif target.exists() and not target.is_dir():
                raise RuntimeError(
                    f"GitHub Pages destination is not a directory [{target}]"
                )
            target.mkdir(parents=True, exist_ok=True)

            for item in source.iterdir():
                destination = target / item.name
                if item.is_dir():
                    shutil.copytree(
                        item, destination, dirs_exist_ok=not clean_destination
                    )
                else:
                    shutil.copy2(item, destination)
            if not any(target.iterdir()):
                raise RuntimeError(
                    f"No GitHub Pages files copied from [{source}] to [{target}]"
                )

            if no_jekyll:
                (worktree / ".nojekyll").touch()
            if update_root_index and destination_dir:
                cls._write_gh_pages_index(worktree)

            Shell.check(
                "git -C "
                f"{shlex.quote(str(worktree))} config user.name "
                f"{shlex.quote(git_user_name)}",
                strict=True,
                verbose=verbose,
            )
            Shell.check(
                "git -C "
                f"{shlex.quote(str(worktree))} config user.email "
                f"{shlex.quote(git_user_email)}",
                strict=True,
                verbose=verbose,
            )
            Shell.check(
                f"git -C {shlex.quote(str(worktree))} add -A",
                strict=True,
                verbose=verbose,
            )
            force_add_path = "." if target == worktree else str(target.relative_to(worktree))
            Shell.check(
                f"git -C {shlex.quote(str(worktree))} add -f -A -- {shlex.quote(force_add_path)}",
                strict=True,
                verbose=verbose,
            )
            if (
                Shell.run(
                    f"git -C {shlex.quote(str(worktree))} diff --cached --quiet",
                    verbose=verbose,
                )
                == 0
            ):
                print("No GitHub Pages changes to publish")
                return cls.gh_pages_url(repo=repo, destination_dir=destination_dir)

            commit_message = (
                commit_message or f"Publish GitHub Pages from {source.name}"
            )
            Shell.check(
                f"git -C {shlex.quote(str(worktree))} commit -m {shlex.quote(commit_message)}",
                strict=True,
                verbose=verbose,
            )
            Shell.check(
                f"git -C {shlex.quote(str(worktree))} push {safe_remote_name} HEAD:{safe_branch}",
                strict=True,
                verbose=verbose,
                env=git_env,
            )
            url = cls.gh_pages_url(repo=repo, destination_dir=destination_dir)
            print(f"Published GitHub Pages: {url}")
            return url
        finally:
            if worktree.exists():
                Shell.run(
                    f"git worktree remove --force {shlex.quote(str(worktree))}",
                    verbose=verbose,
                )
            if remote_added:
                Shell.run(
                    f"git remote remove {safe_remote_name}",
                    verbose=verbose,
                    env=git_env,
                )
            shutil.rmtree(temp_root, ignore_errors=True)

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
    def get_output_with_retries(cls, command, verbose=False):
        """Run a read-style ``gh`` command and return its stdout.

        Mirrors :meth:`do_command_with_retries` but returns the captured
        stdout instead of a boolean.
        """
        retry_count = 0
        out, err = "", ""

        while retry_count < Settings.MAX_RETRIES_GH:
            ret_code, out, err = Shell.get_res_stdout_stderr(command, verbose=verbose)
            if ret_code == 0:
                return out
            if "Validation Failed" in err:
                print(f"ERROR: GH command validation error {[err]}")
                break
            if "Bad credentials" in err:
                print("ERROR: GH credentials/auth failure")
                break
            if "Resource not accessible" in err:
                print("ERROR: GH permissions failure")
                break
            retry_count += 1
            delay = min(2 ** (retry_count + 1), 60)
            time.sleep(delay)

        print(
            f"ERROR: Failed to execute gh command [{command}] out:[{out}] err:[{err}] after [{retry_count}] attempts"
        )
        return ""

    @classmethod
    def _gh_graphql_json(cls, query, variables, verbose=False):
        """Run a GraphQL query via ``gh api graphql`` and return parsed JSON."""
        parts = [f"gh api graphql -f query={shlex.quote(query)}"]
        for k, v in variables.items():
            if isinstance(v, bool):
                parts.append(f"-F {k}={'true' if v else 'false'}")
            elif isinstance(v, int):
                parts.append(f"-F {k}={int(v)}")
            else:
                parts.append(f"-f {k}={shlex.quote(str(v))}")
        cmd = " ".join(parts)
        out = cls.get_output_with_retries(cmd, verbose=verbose)
        if not out:
            raise RuntimeError(f"gh api graphql failed (no output) for cmd [{cmd}]")
        try:
            data = json.loads(out)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"gh api graphql returned non-JSON output [{out[:200]}]: {e}"
            )
        if "errors" in data and data["errors"]:
            raise RuntimeError(f"gh api graphql returned errors: {data['errors']}")
        return data

    @classmethod
    def list_pr_review_threads(cls, pr=None, repo=None, verbose=False):
        """Return all review threads on a PR via GraphQL.

        Each thread carries its node ``id``, ``isResolved``, ``isOutdated``,
        ``resolvedBy`` (``{login}`` or ``null``), ``path``, ``line``, and the
        full list of comments under it. Both the thread list and each thread's
        comments are paginated.
        """
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER
        owner, name = repo.split("/", 1)

        thread_query = (
            "query($owner:String!,$name:String!,$pr:Int!,$after:String){"
            "repository(owner:$owner,name:$name){"
            "pullRequest(number:$pr){"
            "reviewThreads(first:100,after:$after){"
            "pageInfo{hasNextPage endCursor}"
            "nodes{id isResolved isOutdated resolvedBy{login} path line "
            "comments(first:50){"
            "pageInfo{hasNextPage endCursor}"
            "nodes{databaseId createdAt author{login} body path line originalLine}"
            "}}}}}}"
        )
        comments_query = (
            "query($id:ID!,$after:String!){"
            "node(id:$id){... on PullRequestReviewThread{"
            "comments(first:50,after:$after){"
            "pageInfo{hasNextPage endCursor}"
            "nodes{databaseId createdAt author{login} body path line originalLine}"
            "}}}}"
        )

        threads = []
        thread_cursor = None
        while True:
            variables = {"owner": owner, "name": name, "pr": int(pr)}
            if thread_cursor is not None:
                variables["after"] = thread_cursor
            data = cls._gh_graphql_json(thread_query, variables, verbose=verbose)
            page = data["data"]["repository"]["pullRequest"]["reviewThreads"]
            for thread in page["nodes"]:
                comments = thread["comments"]
                while comments["pageInfo"]["hasNextPage"]:
                    sub = cls._gh_graphql_json(
                        comments_query,
                        {"id": thread["id"], "after": comments["pageInfo"]["endCursor"]},
                        verbose=verbose,
                    )
                    next_page = sub["data"]["node"]["comments"]
                    comments["nodes"].extend(next_page["nodes"])
                    comments["pageInfo"] = next_page["pageInfo"]
                threads.append(thread)
            if not page["pageInfo"]["hasNextPage"]:
                break
            thread_cursor = page["pageInfo"]["endCursor"]
        return threads

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

    '''
    TODO: @maxknv
    The fact that a comment can get lost is also an issue for other CI automated comments. 
    I think it makes sense to make this the default behavior for post_updateable_comment() and avoid introducing another method.
    '''
    @classmethod
    def post_fresh_comment(
        cls,
        tag: str,
        body: str,
        pr=None,
        repo=None,
        verbose=True,
    ):
        """Delete any existing comment with the given tag and post a new one at the bottom.

        Unlike post_updateable_comment, this always creates a fresh comment so it
        appears as the most recent comment (next to the merge button).
        """
        if not repo:
            repo = _Environment.get().REPOSITORY
        if not pr:
            pr = _Environment.get().PR_NUMBER

        TAG_START = f"<!-- CI automatic comment start :{tag}: -->"
        TAG_END = f"<!-- CI automatic comment end :{tag}: -->"

        # Fetch all comments and delete those carrying our tag.
        cmd_list = (
            f'gh api -H "Accept: application/vnd.github.v3+json" '
            f'"/repos/{repo}/issues/{pr}/comments" '
            f"--jq '[.[] | {{id: .id, body: .body}}]' --paginate"
        )
        output = Shell.get_output(cmd_list, verbose=verbose)
        if output:
            try:
                for comment in json.loads(output):
                    if TAG_START in comment["body"] and TAG_END in comment["body"]:
                        comment_id = comment["id"]
                        if verbose:
                            print(f"Deleting old coverage comment [{comment_id}]")
                        Shell.run(
                            f'gh api -X DELETE '
                            f'-H "Accept: application/vnd.github.v3+json" '
                            f'"/repos/{repo}/issues/comments/{comment_id}"',
                            verbose=verbose,
                        )
            except Exception as e:
                print(f"WARNING: Failed to delete old comment: {e}")

        # Post a new comment at the bottom.
        full_body = f"{TAG_START}\n{body}\n{TAG_END}\n"
        temp_file_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", delete=False, suffix=".txt", encoding="utf-8"
            ) as temp_file:
                temp_file.write(full_body)
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

        if not pr or not str(pr).isdigit() or int(pr) <= 0:
            print(
                f"WARNING: post_updateable_comment called without a valid PR number "
                f"(got {pr!r}); skipping comment update"
            )
            return False

        TAG_COMMENT_START = "<!-- CI automatic comment start :{TAG}: -->"
        TAG_COMMENT_END = "<!-- CI automatic comment end :{TAG}: -->"
        cmd_check_created = f'gh api -H "Accept: application/vnd.github.v3+json" \
            "/repos/{repo}/issues/{pr}/comments" \
            --jq \'[.[] | {{id: .id, body: .body}}]\' --paginate'
        output = Shell.get_output(cmd_check_created, verbose=verbose)

        if not output or not output.strip():
            print(
                "WARNING: gh api returned no output when listing PR comments — "
                "skipping comment update (likely unauthenticated or rate-limited)"
            )
            return False
        try:
            comments = json.loads(output)
        except json.JSONDecodeError as e:
            print(
                f"WARNING: failed to parse gh api response as JSON ({e}); "
                f"output starts with: {output[:200]!r}"
            )
            return False

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
                    replacement = f"{start_tag}\n{tag_body}\n{end_tag}"
                    body, _ = rex.subn(lambda _: replacement, body)
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

    _STATUS_TO_GH = {
        Result.Status.OK: Result.GHStatus.SUCCESS,
        Result.Status.FAIL: Result.GHStatus.FAILURE,
        Result.Status.ERROR: Result.GHStatus.ERROR,
        Result.Status.SKIPPED: Result.GHStatus.SUCCESS,
        Result.Status.PENDING: Result.GHStatus.PENDING,
        Result.Status.RUNNING: Result.GHStatus.PENDING,
        Result.Status.DROPPED: Result.GHStatus.ERROR,
        Result.Status.UNKNOWN: Result.GHStatus.FAILURE,
        Result.Status.XFAIL: Result.GHStatus.SUCCESS,
        Result.Status.XPASS: Result.GHStatus.FAILURE,
    }

    @classmethod
    def convert_to_gh_status(cls, status):
        """Map Result.Status value to GitHub commit status API string."""
        gh = cls._STATUS_TO_GH.get(status)
        if gh is not None:
            return gh
        # Already a GH status string — pass through for idempotency
        _GH_VALUES = set(cls._STATUS_TO_GH.values())
        assert status in _GH_VALUES, f"Invalid status [{status}] for GH commit status"
        return status

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
        # Outside GitHub Actions GITHUB_EVENT_PATH is empty, which would turn
        # `cat $GITHUB_EVENT_PATH` into a bare `cat` that hangs reading from
        # the controlling TTY. Skip the whole dump in that case — it's GHA
        # debug info and useless without the real env.
        if not os.getenv("GITHUB_ACTIONS"):
            return
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
        extra_links: List[tuple] = dataclasses.field(default_factory=list)

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

            def extract_label_links_md(res: Result) -> str:
                """Render labels with links as markdown ``[name](link)`` chips.
                Reads the unified ``ext['labels']`` and falls back to legacy
                ``ext['hlabels']`` for results stored before the unification.
                """
                try:
                    if not (hasattr(res, "ext") and isinstance(res.ext, dict)):
                        return ""
                    links = []
                    for item in res.ext.get("labels", []) or []:
                        if isinstance(item, dict) and item.get("name") and item.get("link"):
                            links.append(f"[{item['name']}]({item['link']})")
                    for item in res.ext.get("hlabels", []) or []:
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            text, href = item[0], item[1]
                            if text and href:
                                links.append(f"[{text}]({href})")
                    return ", ".join(links)
                except Exception:
                    return ""

            def has_label_links(res: Result) -> bool:
                if not (hasattr(res, "ext") and isinstance(res.ext, dict)):
                    return False
                if any(
                    isinstance(it, dict) and it.get("link")
                    for it in res.ext.get("labels", []) or []
                ):
                    return True
                return bool(res.ext.get("hlabels"))

            summary = cls(
                name=result.name,
                status=result.status,
                sha=sha or Info().sha,
                start_time=result.start_time,
                duration=result.duration,
                failed_results=[],
                info=extract_label_links_md(result),
                comment=result.ext.get("comment", ""),
            )

            # Filter and sort failed/error subresults by priority
            # Priority: FAIL (0) > ERROR (1) > others (2)
            def get_status_priority(r):
                if r.status == Result.Status.FAIL:
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
                    info=extract_label_links_md(sub_result),
                    comment=sub_result.ext.get("comment", ""),
                )
                failed_result.failed_results = [
                    cls(
                        name=r.name,
                        status=r.status,
                        info=extract_label_links_md(r),
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
            def _shared_job_label(names):
                if len(names) == 1:
                    return names[0]
                common = os.path.commonprefix(names)
                for sep in (" (", ", ", "("):
                    idx = common.rfind(sep)
                    if idx > 0:
                        common = common[:idx]
                        break
                common = common.rstrip(" ,(-")
                return common or f"{names[0]} (+{len(names) - 1} more)"

            def _url_key(res):
                urls = []
                for item in (getattr(res, "ext", {}) or {}).get("labels", []) or []:
                    if isinstance(item, dict) and item.get("link"):
                        urls.append(item["link"])
                for item in (getattr(res, "ext", {}) or {}).get("hlabels", []) or []:
                    if isinstance(item, (list, tuple)) and len(item) >= 2 and item[1]:
                        urls.append(item[1])
                return tuple(sorted(urls))

            groups = {}
            group_order = []
            for job_result in getattr(result, "results", []) or []:
                if not has_label_links(job_result):
                    continue
                links_md = extract_label_links_md(job_result)
                if not links_md:
                    continue
                key = _url_key(job_result)
                if key not in groups:
                    groups[key] = {"names": [], "links_md": links_md}
                    group_order.append(key)
                groups[key]["names"].append(job_result.name)
            for key in group_order:
                group = groups[key]
                summary.extra_links.append(
                    (_shared_job_label(group["names"]), group["links_md"])
                )
            return summary

        def to_markdown(self, pr_number=0, sha="", workflow_name="", branch=""):
            def escape_pipes(text):
                """Escape special markdown characters for table cells"""
                return str(text).replace("|", "\\|").replace("#", "\\#")

            if self.status == Result.Status.OK:
                symbol = "✅"  # Green check mark
            elif self.status == Result.Status.FAIL:
                symbol = "❌"  # Red cross mark
            else:
                symbol = "⏳"  # Hourglass (in progress)

            body = f"**Summary:** {symbol}\n"
            if self.extra_links:
                for job_name, links_md in self.extra_links:
                    body += f"- {job_name}: {links_md}\n"
                body += "\n"
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
                                escape_pipes(sub_failed_result.name),
                                sub_failed_result.status,
                                escape_pipes(sub_failed_result.info or ""),
                                escape_pipes(sub_failed_result.comment or ""),
                            )
            return body


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="GitHub PR comment helper")
    subparsers = parser.add_subparsers(dest="command")

    post_parser = subparsers.add_parser(
        "post-or-update",
        help="Post a new PR comment or update an existing one with the given tag",
    )
    post_parser.add_argument(
        "--tag",
        required=True,
        help="Tag identifying the comment section (e.g. 'review')",
    )
    post_parser.add_argument(
        "--file",
        required=True,
        dest="body_file",
        help="Path to file containing the comment body",
    )
    post_parser.add_argument("--pr", type=int, default=None, help="PR number")
    post_parser.add_argument(
        "--repo", default=None, help="Repository in owner/repo format"
    )
    post_parser.add_argument(
        "--only-update",
        action="store_true",
        help="Only update an existing comment; do not create a new one",
    )

    args = parser.parse_args()

    if args.command == "post-or-update":
        with open(args.body_file, "r", encoding="utf-8") as f:
            body = f.read()
        kwargs = dict(
            comment_tags_and_bodies={args.tag: body},
            only_update=args.only_update,
        )
        if args.pr is not None:
            kwargs["pr"] = args.pr
        if args.repo is not None:
            kwargs["repo"] = args.repo
        ok = GH.post_updateable_comment(**kwargs)
        sys.exit(0 if ok else 1)
    else:
        parser.print_help()
        sys.exit(1)
