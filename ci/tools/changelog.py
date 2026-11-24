#!/usr/bin/env python3
# In our CI this script runs in style-test containers

import argparse
import logging
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from subprocess import DEVNULL
from typing import Any, Dict, List, Optional, Sequence, Set, TextIO, Tuple, Union

import tqdm  # type: ignore
from github.GithubException import RateLimitExceededException, UnknownObjectException
from github.NamedUser import NamedUser
from rapidfuzz.distance import Levenshtein  # type: ignore

from git_helper import git_runner, is_shallow
from github_helper import GitHub, Issues, PullRequest, PullRequests, Repository
from ci.jobs.scripts.clickhouse_version import (
    FILE_WITH_VERSION_PATH,
    get_abs_path,
    get_version_from_repo,
    get_version_from_tag,
)

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ci.praktika.utils import Shell

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


def _normalize_for_matching(cat: str) -> str:
    """Normalize category for matching: strip parenthetical suffix, collapse whitespace, casefold."""
    pos = cat.find("(")
    result = cat[:pos] if pos != -1 else cat
    return re.sub(r"\s+", " ", result.strip()).casefold()


# Maximum normalized Levenshtein distance for fuzzy matching (20%)
MAX_NORMALIZED_DISTANCE = 0.2


# Categories that should be skipped in the changelog, grouped by skip reason.
# Uses the same canonical names as LABEL_CATEGORIES in pr_labels_and_category.py.
_SKIP_CATEGORIES = {
    "documentation": ["Documentation"],
    "not-for-changelog": [
        "Not for changelog",
        "CI Fix or Improvement",
    ],
}

# Also match historical/legacy category names not present in LABEL_CATEGORIES.
_SKIP_LEGACY_PATTERN = re.compile(
    r"(?i)((non|in|not|un)[-\s]*significant)"
    r"|(changelog[ ]*entry[ ]*is[ ]*not[ ]*required)"
)


def _match_skip_category(category: str) -> Optional[str]:
    """Check if a category should be skipped from the changelog.

    Returns the skip reason ("documentation" or "not-for-changelog") or None.
    Uses normalized Levenshtein matching with the same 20% threshold.
    """
    norm = _normalize_for_matching(category)
    compact = norm.replace(" ", "")

    for reason, names in _SKIP_CATEGORIES.items():
        for name in names:
            name_norm = _normalize_for_matching(name)
            if name_norm == norm:
                return reason
            name_compact = name_norm.replace(" ", "")
            if Levenshtein.normalized_distance(compact, name_compact) <= MAX_NORMALIZED_DISTANCE:
                return reason

    # Handle legacy names that don't match any current category
    if _SKIP_LEGACY_PATTERN.search(category):
        return "not-for-changelog"

    return None


def _match_changelog_category(category: str) -> Optional[str]:
    """Match a PR category to a preferred changelog category.

    Uses the same rules as the CI check: case-insensitive, whitespace-insensitive,
    normalized Levenshtein distance up to 20% (with whitespace removed for comparison).
    """
    norm = _normalize_for_matching(category)
    compact = norm.replace(" ", "")

    # Exact normalized match first
    for pref in categories_preferred_order:
        if _normalize_for_matching(pref) == norm:
            return pref

    # Fuzzy match with normalized Levenshtein distance <= 20%
    best_dist = MAX_NORMALIZED_DISTANCE + 1e-9
    best_match = None
    for pref in categories_preferred_order:
        pref_compact = _normalize_for_matching(pref).replace(" ", "")
        dist = Levenshtein.normalized_distance(compact, pref_compact)
        if dist < best_dist:
            best_dist = dist
            best_match = pref

    return best_match


BOT_AUTHORS = {"dependabot[bot]"}

# The subject GitHub gives to the merge commit of a pull request: its number, and
# its head branch after the owner.
MERGE_SUBJECT_RE = re.compile(r"^Merge pull request #(\d+) from [^/\s]+/(\S+)")

FROM_REF = ""
TO_REF = ""
SHA_IN_CHANGELOG = []  # type: List[str]
gh = GitHub(create_cache_dir=False)
runner = git_runner


def link_issues(text: str) -> str:
    """Substitute issue links."""
    # 1) issue number w/o markdown link
    text = re.sub(
        r"([^[])#([0-9]{4,})",
        r"\1[#\2](https://github.com/ClickHouse/ClickHouse/issues/\2)",
        text,
    )
    # 2) issue URL w/o markdown link
    # including #issuecomment-1 or #event-12
    return re.sub(
        r"([^(])(https://github.com/ClickHouse/ClickHouse/issues/([0-9]{4,})[-#a-z0-9]*)",
        r"\1[#\3](\2)",
        text,
    )


def get_backported_number(head_ref: str) -> Optional[int]:
    """The number of the original pull request, if `head_ref` is the branch of
    its backport: `backport/<release branch>/<number>`."""
    parts = head_ref.split("/")
    if len(parts) == 3 and parts[0] == "backport" and parts[2].isdecimal():
        return int(parts[2])
    return None


class Description:
    def __init__(
        self, number: int, user: NamedUser, html_url: str, entry: str, category: str
    ):
        self.number = number
        self.html_url = html_url
        user_login = user._rawData["login"]
        if user_login == "Copilot":

            class CopilotUser:
                name = "Copilot"
                login = "Copilot"
                html_url = ""

            self.user = CopilotUser()
        else:
            self.user = gh.get_user_cached(user_login)  # type: ignore
        self.entry = entry
        self.category = category

    @property
    def formatted_entry(self) -> str:
        entry = link_issues(self.entry)
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
        user_display = (
            f"[{user_name}]({self.user.html_url})" if self.user.html_url else user_name
        )
        return f"* {entry} [#{self.number}]({self.html_url}) ({user_display})."

    # Sort PR descriptions by numbers
    def __eq__(self, other: Any) -> bool:
        if not isinstance(self, type(other)):
            raise NotImplementedError
        return bool(self.number == other.number)

    def __lt__(self, other: "Entry") -> bool:
        return self.number < other.number


class ExistingEntry:
    """An entry copied from `CHANGELOG.md` as it is, see `get_existing_entries`."""

    def __init__(self, number: int, category: str, formatted_entry: str):
        self.number = number
        self.category = category
        self.formatted_entry = formatted_entry

    def __lt__(self, other: "Entry") -> bool:
        return self.number < other.number


Entry = Union[Description, ExistingEntry]


def read_existing_entries(path: Path, repo: str) -> Dict[int, List[Tuple[str, str]]]:
    """The entries of `CHANGELOG.md` by pull request, as `(category, bullet)`.

    A bullet is an entry of every pull request it attributes. The attribution is
    what this script writes at the end of an entry, and what an editor keeps for
    each of the entries merged into one bullet: `[#N](.../pull/N) ([Author](...))`.
    A link that is not followed by the author is a mention in the text of another
    entry ("Follow-up to ..."), not an entry of that pull request. It is the same
    definition as `PR_ATTRIBUTION_RE` in `ci/jobs/changelog_nightly.py`.
    """
    attribution = re.compile(
        rf"\[#(\d+)\]\(https://github\.com/{re.escape(repo)}/pull/\1\) \(\["
    )
    entries = {}  # type: Dict[int, List[Tuple[str, str]]]
    category = ""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("#### "):
            category = line[5:].strip()
            category = _match_changelog_category(category) or category
        elif line.startswith("#"):
            # A header of another level ends the category
            category = ""
        elif line.startswith("* ") and category:
            for number in dict.fromkeys(attribution.findall(line)):
                entries.setdefault(int(number), []).append((category, line.rstrip()))
    return entries


def get_merged_prs() -> Dict[int, str]:
    """The pull requests merged in the changelog interval according to git, as
    number -> head branch.

    GitHub merges a pull request with a commit `Merge pull request #N from
    owner/branch` on the first-parent history of the base branch. A pull request
    merged in another way has no such commit and is not found here.
    """
    merged = {}  # type: Dict[int, str]
    subjects = runner.run(
        f"git log --first-parent --format=format:%s {FROM_REF}..{TO_REF}"
    )
    for subject in subjects.split("\n"):
        match = MERGE_SUBJECT_RE.match(subject)
        if match:
            merged.setdefault(int(match.group(1)), match.group(2))
    return merged


def get_existing_entries(
    issues: Issues, path: Path, repo: str
) -> Tuple[List[ExistingEntry], Set[int]]:
    """The entries to copy from `CHANGELOG.md`, and the pull requests they stand
    for: the ones that do not have to be requested from GitHub.

    The search finds the pull requests by the date of the merge, and the dates
    cover more than the changelog interval, so an entry is copied only for a pull
    request whose merge commit is in the interval. For a backport, it is the entry
    of the original pull request. A pull request without an entry, or without a
    merge commit found in the interval, is left to `get_descriptions`.
    """
    existing = read_existing_entries(path, repo)
    merged = get_merged_prs()
    entries = {}  # type: Dict[Tuple[str, str], ExistingEntry]
    reused = set()  # type: Set[int]
    for number in sorted(issue.number for issue in issues):
        head_ref = merged.get(number)
        if head_ref is None:
            continue
        original = get_backported_number(head_ref)
        bullets = existing.get(number if original is None else original)
        if not bullets:
            continue
        reused.add(number)
        for category, bullet in bullets:
            # The same prefix and the same place as in `generate_description`:
            # a backport is sorted by the number of the original pull request.
            entry_number = number
            if original is not None:
                prefix = link_issues(f"Backported in #{number}: ")
                bullet = f"* {prefix}{bullet[2:]}"
                entry_number = original
            # A bullet that attributes several pull requests is their common
            # entry: it is written once, at the place of the first of them.
            entries.setdefault(
                (category, bullet), ExistingEntry(entry_number, category, bullet)
            )
    return list(entries.values()), reused


def get_descriptions(
    prs: PullRequests, existing_entries: Sequence[ExistingEntry] = ()
) -> Dict[str, List[Entry]]:
    descriptions = {}  # type: Dict[str, List[Entry]]
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

    for entry in existing_entries:
        descriptions.setdefault(entry.category, []).append(entry)

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
        "--no-fetch",
        action="store_true",
        help="do not unshallow the repository and do not fetch tags; the caller "
        "has already fetched the history and the tags needed for the interval",
    )
    parser.add_argument(
        "--reuse-entries-from",
        type=Path,
        metavar="CHANGELOG",
        help="a path to `CHANGELOG.md`: a pull request that already has an entry "
        "there is not requested from GitHub, the entry is copied as it is",
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


# This function mirrors the PR description checks in ClickHousePullRequestTrigger.
# Returns None if the PR should not be mentioned in changelog.
def generate_description(item: PullRequest, repo: Repository) -> Optional[Description]:
    backport_number = item.number
    if item.head.ref.startswith("backport/"):
        original_number = get_backported_number(item.head.ref)
        if original_number is not None:
            try:
                item = gh.get_pull_cached(repo, original_number)
            except Exception as e:
                logging.warning("unable to get backported PR, exception: %s", e)
        else:
            logging.warning(
                "The branch %s doesn't match backport template, using PR %s as is",
                item.head.ref,
                item.number,
            )
    # Skip PRs by bot authors (e.g., dependabot)
    # pylint: disable=protected-access
    user_login = item.user._rawData["login"]
    # pylint: enable=protected-access
    if user_login in BOT_AUTHORS:
        logging.info("Skipping PR %s by bot author '%s'", item.number, user_login)
        return None

    description = item.body
    # Strip the auto-maintained version-info block (added by pr_version_info.py)
    # so it can never leak into the changelog entry: when the entry is empty it
    # is the first content after the header and would otherwise be parsed as the
    # entry. The markers are HTML comments, invisible in the rendered body.
    if description:
        description = re.sub(
            r"<!-- ch-version-info:start -->.*?<!-- ch-version-info:end -->",
            "",
            description,
            flags=re.DOTALL,
        )
    # Don't skip empty lines because they delimit parts of description
    lines = [x.strip() for x in (description.split("\n") if description else [])]
    lines = [re.sub(r"\s+", " ", ln) for ln in lines]

    category = ""
    entry = ""

    if lines:
        i = 0
        while i < len(lines):
            m_cat = re.match(
                r"(?i)^[#>*_ ]*change\s*log\s*category(?:[^:]*:\s*(.*))?$",
                lines[i],
            )
            m_entry = re.match(
                r"(?i)^[#>*_ ]*(short\s*description|change\s*log\s*entry)(?:\s*\(.*\))?(?:[^:]*:\s*(.*))?$",
                lines[i],
            )
            if m_cat:
                # Check if the category is on the same line
                inline = (m_cat.group(1) or "").strip()
                inline = re.sub(r"^[-*_\s]*", "", inline)
                inline = re.sub(r"[-*_\s]*$", "", inline)
                if inline:
                    new_category = inline
                    i += 1
                else:
                    i += 1
                    if i >= len(lines):
                        break
                    # Can have one empty line between header and the category itself.
                    # Filter it out.
                    if not lines[i]:
                        i += 1
                        if i >= len(lines):
                            break
                    new_category = re.sub(r"^[-*\s]*", "", lines[i])
                    i += 1
                # If we already found a category, this is a duplicate — skip
                # silently (changelog.py doesn't report errors, just picks the first).
                if not category:
                    category = new_category
            elif m_entry:
                # Check if the entry is on the same line
                inline = (m_entry.group(2) or "").strip()
                # Strip markdown formatting markers (e.g. "**Changelog entry:**" yields "**")
                inline = re.sub(r"^[-*_\s]*", "", inline)
                inline = re.sub(r"[-*_\s]*$", "", inline)
                if inline:
                    entry_lines = [inline]
                    i += 1
                else:
                    i += 1
                    # Can have one empty line between header and the entry itself.
                    # Filter it out.
                    if i < len(lines) and not lines[i]:
                        i += 1
                    entry_lines = []
                # All following lines until empty one are the changelog entry.
                while i < len(lines) and lines[i]:
                    entry_lines.append(lines[i])
                    i += 1
                entry = " ".join(entry_lines)
            else:
                i += 1

    # Remove excessive bullets from the entry.
    if re.match(r"^[\-\*] ", entry):
        entry = entry[2:]

    # Better style. Uppercase only the first letter (str.capitalize lowercases
    # the rest of the string, mangling URLs and identifiers).
    if re.match(r"^[a-z]", entry):
        entry = entry[0].upper() + entry[1:]

    if not category:
        # Shouldn't happen, because description check in CI should catch such PRs.
        # Fall through, so that it shows up in output and the user can fix it.
        category = "NO CL CATEGORY"

    # Filter out categories that are not for changelog, using the same
    # fuzzy matching as the CI check.
    skip_label = _match_skip_category(category)
    if skip_label == "documentation":
        return None
    if skip_label == "not-for-changelog":
        category = "NOT FOR CHANGELOG / INSIGNIFICANT"
        # Sometimes we declare not for changelog but still write a description. Keep it
        if len(entry) <= 4 or "Documentation entry" in entry:
            entry = item.title

    # Normalize bug fixes
    if (
        re.match(
            r"(?i).*bug\Wfix",
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

    matched = _match_changelog_category(category)
    if matched:
        category = matched

    return Description(item.number, item.user, item.html_url, entry, category)


def write_changelog(
    fd: TextIO, descriptions: Dict[str, List[Entry]], year: int
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
    global FROM_REF, TO_REF  # pylint:disable=global-statement
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
    temp_cmake = "ci/tools/tmp/autogenerated_versions.txt"
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
    global SHA_IN_CHANGELOG  # pylint:disable=global-statement
    SHA_IN_CHANGELOG = runner.run(
        f"git log --format=format:%H {FROM_REF}..{TO_REF}"
    ).split("\n")


def get_year(issues: Issues) -> int:
    if not issues:
        return date.today().year
    return max(issue.created_at.year for issue in issues)


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

    if args.no_fetch:
        if is_shallow():
            raise RuntimeError(
                "--no-fetch is given, but the repository is shallow: "
                "fetch the history and tags before running the script"
            )
    else:
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
    global gh  # pylint:disable=global-statement
    gh = GitHub(
        args.gh_user_or_token,
        args.gh_password,
        create_cache_dir=False,
        per_page=100,
        pool_size=args.jobs,
    )
    query = f"type:pr repo:{args.repo} is:merged"

    branch, patch = get_branch_and_patch_by_tag(TO_REF)
    if branch and patch and Shell.check(f"git show-ref --quiet {branch}"):
        if patch > 1:
            query += f" base:{branch}"
            logging.info(
                "NOTE: It's a patch [%s]. will use base branch to filter PRs [%s]",
                patch,
                branch,
            )
        else:
            logging.info(
                "NOTE: It's a first patch version. should count PRs merged on master - won't filter PRs by branch"
            )
    else:
        logging.error("ERROR: invalid branch %s - pass", branch)

    logging.info("Fetch PRs with query %s", query)
    # The search returns issues: getting a pull request costs one more request
    # for each of them, and writing its entry yet another one for the author.
    issues = gh.search_issues(query=query, merged=merged, sort="created")

    existing_entries = []  # type: List[ExistingEntry]
    reused = set()  # type: Set[int]
    if args.reuse_entries_from:
        existing_entries, reused = get_existing_entries(
            issues, args.reuse_entries_from, args.repo
        )
        logging.info(
            "%s of %s pull requests already have an entry in %s: "
            "copying it instead of requesting them from GitHub",
            len(reused),
            len(issues),
            args.reuse_entries_from,
        )

    prs = gh.get_pulls_from_issues(
        [issue for issue in issues if issue.number not in reused],
        progress_func=tqdm.tqdm,
    )

    descriptions = get_descriptions(prs, existing_entries)
    changelog_year = get_year(issues)

    write_changelog(args.output, descriptions, changelog_year)


if __name__ == "__main__":
    main()
