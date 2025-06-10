#!/usr/bin/env python3
# In our CI this script runs in style-test containers

import argparse
import logging
import re
from datetime import date, timedelta
from pathlib import Path
from subprocess import DEVNULL
from typing import Any, Dict, List, Optional, TextIO, Tuple

import tqdm  # type: ignore
from github.GithubException import RateLimitExceededException, UnknownObjectException
from github.NamedUser import NamedUser
from thefuzz.fuzz import ratio  # type: ignore

from cache_utils import GitHubCache
from env_helper import TEMP_PATH
from git_helper import git_runner, is_shallow
from github_helper import GitHub, PullRequest, PullRequests, Repository
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from ci_utils import Shell
from version_helper import (
    FILE_WITH_VERSION_PATH,
    get_abs_path,
    get_version_from_repo,
    get_version_from_tag,
)

# This array gives the preferred category order, and is also used to
# normalize category names.
# Categories are used in .github/PULL_REQUEST_TEMPLATE.md, keep comments there
# updated accordingly
categories_preferred_order = (
    "Backward Incompatible Change",
    "New Feature",
    "Experimental Feature",
    "Performance Improvement",
    "Improvement",
    # "Critical Bug Fix (crash, LOGICAL_ERROR, data loss, RBAC)",
    "Bug Fix (user-visible misbehavior in an official stable release)",
    "Build/Testing/Packaging Improvement",
    "Other",
)

FROM_REF = ""
TO_REF = ""
SHA_IN_CHANGELOG = []  # type: List[str]
gh = GitHub(create_cache_dir=False)
runner = git_runner


class Description:
    def __init__(
        self, number: int, user: NamedUser, html_url: str, entry: str, category: str
    ):
        self.number = number
        self.html_url = html_url
        self.user = gh.get_user_cached(user._rawData["login"])  # type: ignore
        self.entry = entry
        self.category = category

    @property
    def formatted_entry(self) -> str:
        # Substitute issue links.
        # 1) issue number w/o markdown link
        entry = re.sub(
            r"([^[])#([0-9]{4,})",
            r"\1[#\2](https://github.com/ClickHouse/ClickHouse/issues/\2)",
            self.entry,
        )
        # 2) issue URL w/o markdown link
        # including #issuecomment-1 or #event-12
        entry = re.sub(
            r"([^(])(https://github.com/ClickHouse/ClickHouse/issues/([0-9]{4,})[-#a-z0-9]*)",
            r"\1[#\3](\2)",
            entry,
        )
        # It's possible that we face a secondary rate limit.
        # In this case we should sleep until we get it
        while True:
            try:
                user_name = self.user.name if self.user.name else self.user.login
                break
            except UnknownObjectException:
                user_name = self.user.login
                break
            except RateLimitExceededException:
                gh.sleep_on_rate_limit()
        return (
            f"* {entry} [#{self.number}]({self.html_url}) "
            f"([{user_name}]({self.user.html_url}))."
        )

    # Sort PR descriptions by numbers
    def __eq__(self, other: Any) -> bool:
        if not isinstance(self, type(other)):
            raise NotImplementedError
        return bool(self.number == other.number)

    def __lt__(self, other: "Description") -> bool:
        return self.number < other.number


def get_descriptions(prs: PullRequests) -> Dict[str, List[Description]]:
    descriptions = {}  # type: Dict[str, List[Description]]
    repos = {}  # type: Dict[str, Repository]
    for pr in prs:
        # See https://github.com/PyGithub/PyGithub/issues/2202,
        # obj._rawData doesn't spend additional API requests
        # We'll save some requests
        # pylint: disable=protected-access
        repo_name = pr._rawData["base"]["repo"]["full_name"]
        # pylint: enable=protected-access
        if repo_name not in repos:
            repos[repo_name] = pr.base.repo
        in_changelog = False
        merge_commit = pr.merge_commit_sha
        if merge_commit is None:
            logging.warning("PR %s does not have merge-commit, skipping", pr.number)
            continue

        in_changelog = merge_commit in SHA_IN_CHANGELOG
        if in_changelog:
            desc = generate_description(pr, repos[repo_name])
            if desc:
                if desc.category not in descriptions:
                    descriptions[desc.category] = []
                descriptions[desc.category].append(desc)

    for descs in descriptions.values():
        descs.sort()

    return descriptions


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Generate a changelog in Markdown format between given tags. "
        "It fetches all tags and unshallow the git repository automatically",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="set the script verbosity, could be used multiple",
    )
    parser.add_argument(
        "--debug-helpers",
        action="store_true",
        help="add debug logging for git_helper and github_helper",
    )
    parser.add_argument(
        "--output",
        type=argparse.FileType("w"),
        default="-",
        help="output file for changelog",
    )
    parser.add_argument(
        "--repo",
        default="ClickHouse/ClickHouse",
        help="a repository to query for pull-requests from GitHub",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=10,
        help="number of jobs to get pull-requests info from GitHub API",
    )
    parser.add_argument(
        "--gh-user-or-token",
        help="user name or GH token to authenticate",
        default=get_best_robot_token(),
    )
    parser.add_argument(
        "--gh-password",
        help="a password that should be used when user is given",
    )
    parser.add_argument(
        "--with-testing-tags",
        action="store_true",
        help="by default '*-testing' tags are ignored, this argument enables them too",
    )
    parser.add_argument(
        "--from",
        dest="from_ref",
        help="git ref for a starting point of changelog, by default is calculated "
        "automatically to match a previous tag in history",
    )
    parser.add_argument(
        "to_ref",
        metavar="TO_REF",
        help="git ref for the changelog end",
    )
    args = parser.parse_args()
    return args


# This function mirrors the PR description checks in ClickhousePullRequestTrigger.
# Returns None if the PR should not be mentioned in changelog.
def generate_description(item: PullRequest, repo: Repository) -> Optional[Description]:
    backport_number = item.number
    if item.head.ref.startswith("backport/"):
        branch_parts = item.head.ref.split("/")
        if len(branch_parts) == 3:
            try:
                item = gh.get_pull_cached(repo, int(branch_parts[-1]))
            except Exception as e:
                logging.warning("unable to get backported PR, exception: %s", e)
        else:
            logging.warning(
                "The branch %s doesn't match backport template, using PR %s as is",
                item.head.ref,
                item.number,
            )
    description = item.body
    # Don't skip empty lines because they delimit parts of description
    lines = [x.strip() for x in (description.split("\n") if description else [])]
    lines = [re.sub(r"\s+", " ", ln) for ln in lines]

    category = ""
    entry = ""

    if lines:
        i = 0
        while i < len(lines):
            if re.match(r"(?i)^[#>*_ ]*change\s*log\s*category", lines[i]):
                i += 1
                if i >= len(lines):
                    break
                # Can have one empty line between header and the category itself.
                # Filter it out.
                if not lines[i]:
                    i += 1
                    if i >= len(lines):
                        break
                category = re.sub(r"^[-*\s]*", "", lines[i])
                i += 1
            elif re.match(
                r"(?i)^[#>*_ ]*(short\s*description|change\s*log\s*entry)", lines[i]
            ):
                i += 1
                # Can have one empty line between header and the entry itself.
                # Filter it out.
                if i < len(lines) and not lines[i]:
                    i += 1
                # All following lines until empty one are the changelog entry.
                entry_lines = []
                while i < len(lines) and lines[i]:
                    entry_lines.append(lines[i])
                    i += 1
                entry = " ".join(entry_lines)
            else:
                i += 1

    # Remove excessive bullets from the entry.
    if re.match(r"^[\-\*] ", entry):
        entry = entry[2:]

    # Better style.
    if re.match(r"^[a-z]", entry):
        entry = entry.capitalize()

    if not category:
        # Shouldn't happen, because description check in CI should catch such PRs.
        # Fall through, so that it shows up in output and the user can fix it.
        category = "NO CL CATEGORY"

    # Filter out documentations changelog before not-for-changelog
    if re.match(
        r"(?i)doc",
        category,
    ):
        return None

    # Filter out the PR categories that are not for changelog.
    if re.search(
        r"(?i)((non|in|not|un)[-\s]*significant)|"
        r"(not[ ]*for[ ]*changelog)|"
        r"(changelog[ ]*entry[ ]*is[ ]*not[ ]*required)",
        category,
    ):
        category = "NOT FOR CHANGELOG / INSIGNIFICANT"
        # Sometimes we declare not for changelog but still write a description. Keep it
        if len(entry) <= 4 or "Documentation entry" in entry:
            entry = item.title

    # Normalize bug fixes
    if (
        re.match(
            r"(?i)bug\Wfix",
            category,
        )
        # Map "Critical Bug Fix" to "Bug fix" category for changelog
        # and "Critical Bug Fix" not in category
    ):
        category = "Bug Fix (user-visible misbehavior in an official stable release)"

    if backport_number != item.number:
        entry = f"Backported in #{backport_number}: {entry}"

    if not entry:
        # Shouldn't happen, because description check in CI should catch such PRs.
        category = "NO CL ENTRY"
        entry = "NO CL ENTRY:  '" + item.title + "'"

    entry = entry.strip()
    if entry[-1] != ".":
        entry += "."

    for c in categories_preferred_order:
        if ratio(category.lower(), c.lower()) >= 90:
            category = c
            break

    return Description(item.number, item.user, item.html_url, entry, category)


def write_changelog(
    fd: TextIO, descriptions: Dict[str, List[Description]], year: int
) -> None:
    to_commit = runner(f"git rev-parse {TO_REF}^{{}}")[:11]
    from_commit = runner(f"git rev-parse {FROM_REF}^{{}}")[:11]
    fd.write(
        f"---\nsidebar_position: 1\nsidebar_label: {year}\n---\n\n"
        f"# {year} Changelog\n\n"
        f"### ClickHouse release {TO_REF} ({to_commit}) FIXME "
        f"as compared to {FROM_REF} ({from_commit})\n\n"
    )

    seen_categories = []  # type: List[str]
    for category in categories_preferred_order:
        if category in descriptions:
            seen_categories.append(category)
            fd.write(f"#### {category}\n")
            for desc in descriptions[category]:
                fd.write(f"{desc.formatted_entry}\n")

            fd.write("\n")

    for category in sorted(descriptions):
        if category not in seen_categories:
            fd.write(f"#### {category}\n\n")
            for desc in descriptions[category]:
                fd.write(f"{desc.formatted_entry}\n")

            fd.write("\n")


def check_refs(from_ref: Optional[str], to_ref: str, with_testing_tags: bool) -> None:
    global FROM_REF, TO_REF
    TO_REF = to_ref

    # Check TO_REF
    runner.run(f"git rev-parse {TO_REF}")

    # Check from_ref
    if from_ref is not None:
        runner.run(f"git rev-parse {FROM_REF}")
        FROM_REF = from_ref
        return

    # Get the cmake/autogenerated_versions.txt from FROM_REF to read the version
    # If the previous tag is greater than version in the FROM_REF,
    # then we need to add it to tags_to_exclude
    temp_cmake = "tests/ci/tmp/autogenerated_versions.txt"
    cmake_version = get_abs_path(temp_cmake)
    cmake_version.write_text(runner(f"git show {TO_REF}:{FILE_WITH_VERSION_PATH}"))
    to_ref_version = get_version_from_repo(cmake_version)
    # Get all tags pointing to TO_REF
    excluded_tags = runner.run(f"git tag --points-at '{TO_REF}^{{}}'").split("\n")
    logging.info("All tags pointing to %s:\n%s", TO_REF, excluded_tags)
    if not with_testing_tags:
        excluded_tags.append("*-testing")
    while not from_ref:
        exclude = " ".join([f"--exclude='{tag}'" for tag in excluded_tags])
        from_ref_tag = runner(f"git describe --abbrev=0 --tags {exclude} '{TO_REF}'")
        from_ref_version = get_version_from_tag(from_ref_tag)
        if from_ref_version <= to_ref_version:
            from_ref = from_ref_tag
            break
        excluded_tags.append(from_ref_tag)

    cmake_version.unlink()
    FROM_REF = from_ref


def set_sha_in_changelog():
    global SHA_IN_CHANGELOG
    SHA_IN_CHANGELOG = runner.run(
        f"git log --format=format:%H {FROM_REF}..{TO_REF}"
    ).split("\n")


def get_year(prs: PullRequests) -> int:
    if not prs:
        return date.today().year
    return max(pr.created_at.year for pr in prs)


def get_branch_and_patch_by_tag(tag: str) -> Tuple[Optional[str], Optional[int]]:
    tag = tag.removeprefix("v")
    versions = tag.split(".")
    if len(versions) < 4:
        print("ERROR: Can't get branch by tag")
        return None, None
    try:
        patch_version = int(versions[2])
        branch = f"{int(versions[0])}.{int(versions[1])}"
        print(f"Branch [{branch}], patch version [{patch_version}]")
    except ValueError:
        return None, None
    return branch, patch_version


def main():
    log_levels = [logging.WARN, logging.INFO, logging.DEBUG]
    args = parse_args()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]:\n%(message)s",
        level=log_levels[min(args.verbose, 2)],
    )
    if args.debug_helpers:
        logging.getLogger("github_helper").setLevel(logging.DEBUG)
        logging.getLogger("git_helper").setLevel(logging.DEBUG)

    # Get the full repo
    if is_shallow():
        logging.info("Unshallow repository")
        runner.run("git fetch --unshallow", stderr=DEVNULL)
    logging.info("Fetching all tags")
    runner.run("git fetch --tags", stderr=DEVNULL)

    check_refs(args.from_ref, args.to_ref, args.with_testing_tags)
    set_sha_in_changelog()

    logging.info("Using %s..%s as changelog interval", FROM_REF, TO_REF)

    # use merge-base commit as a starting point, if used ref in another branch
    base_commit = runner.run(f"git merge-base '{FROM_REF}^{{}}' '{TO_REF}^{{}}'")
    # Get starting and ending dates for gathering PRs
    # Add one day after and before to mitigate TZ possible issues
    # `tag^{}` format gives commit ref when we have annotated tags
    # format %cs gives a committer date, works better for cherry-picked commits
    from_date = runner.run(f"git log -1 --format=format:%cs '{base_commit}'")
    to_date = runner.run(f"git log -1 --format=format:%cs '{TO_REF}^{{}}'")
    merged = (
        date.fromisoformat(from_date) - timedelta(1),
        date.fromisoformat(to_date) + timedelta(1),
    )

    # Get all PRs for the given time frame
    global gh
    gh = GitHub(
        args.gh_user_or_token,
        args.gh_password,
        create_cache_dir=False,
        per_page=100,
        pool_size=args.jobs,
    )
    temp_path = Path(TEMP_PATH)
    gh_cache = GitHubCache(gh.cache_path, temp_path, S3Helper())
    gh_cache.download()
    query = f"type:pr repo:{args.repo} is:merged"

    branch, patch = get_branch_and_patch_by_tag(TO_REF)
    if branch and patch and Shell.check(f"git show-ref --quiet {branch}"):
        if patch > 1:
            query += f" base:{branch}"
            print(
                f"NOTE: It's a patch [{patch}]. will use base branch to filter PRs [{branch}]"
            )
        else:
            print(
                f"NOTE: It's a first patch version. should count PRs merged on master - won't filter PRs by branch"
            )
    else:
        print(f"ERROR: invalid branch {branch} - pass")

    print(f"Fetch PRs with query {query}")
    prs = gh.get_pulls_from_search(
        query=query, merged=merged, sort="created", progress_func=tqdm.tqdm
    )

    descriptions = get_descriptions(prs)
    changelog_year = get_year(prs)

    write_changelog(args.output, descriptions, changelog_year)
    gh_cache.upload()


if __name__ == "__main__":
    main()
