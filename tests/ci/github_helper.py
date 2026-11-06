#!/usr/bin/env python
"""Helper for GitHub API requests"""
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from os import path as p
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional, Tuple, Union

import github
import requests

# explicit reimport
# pylint: disable=useless-import-alias
from github.AuthenticatedUser import AuthenticatedUser
from github.GithubException import (
    GithubException as GithubException,
    RateLimitExceededException as RateLimitExceededException,
)
from github.Issue import Issue as Issue
from github.NamedUser import NamedUser as NamedUser
from github.PullRequest import PullRequest as PullRequest
from github.Repository import Repository as Repository

# pylint: enable=useless-import-alias

CACHE_PATH = p.join(p.dirname(p.realpath(__file__)), "gh_cache")

logger = logging.getLogger(__name__)

PullRequests = List[PullRequest]
Issues = List[Issue]

# How many PRs to fetch concurrently in `get_pulls_from_search`. The GitHub
# search API returns issues, and each one still needs a `get_pull` request; those
# requests are independent, so fetching them in parallel turns an O(N) sequence
# of round-trips into a handful of waves.
DEFAULT_FETCH_THREADS = 12


@dataclass
class PullRequestInfo:
    """Lightweight, read-only view of a pull request fetched via GraphQL.

    Carries only the fields needed for bulk classification and body edits, so a
    whole search result set can be retrieved in a few batched GraphQL requests
    instead of one REST request per PR. To mutate a PR (e.g. `PullRequest.edit`)
    fetch the full object via `GitHub.get_pull_cached` for the few that change.
    """

    number: int
    head_ref: str
    base_ref: str
    label_names: List[str]
    merged: bool
    merge_commit_sha: Optional[str]
    body: str
    node_id: str
    html_url: str
    repo: str
    # Numbers of the issues this PR resolves, taken from GitHub's "Development"
    # links (`closingIssuesReferences`), restricted to the PR's own repository.
    closing_issue_numbers: List[int] = field(default_factory=list)


class GitHub(github.Github):
    def __init__(self, *args, create_cache_dir=True, **kwargs):
        # Define meta attribute and apply setter logic
        self._cache_path = Path(CACHE_PATH)
        if create_cache_dir:
            self.cache_path = self.cache_path
        if not kwargs.get("per_page"):
            kwargs["per_page"] = 100
        # And set Path
        super().__init__(*args, **kwargs)
        self._retries = 0

    # pylint: disable=signature-differs
    def search_issues(self, *args, **kwargs) -> Issues:  # type: ignore
        """Wrapper around search method with throttling and splitting by date.

        We split only by the first"""
        splittable_arg = ""
        splittable_value = []
        for arg, value in kwargs.items():
            if arg in ["closed", "created", "merged", "updated"]:
                if hasattr(value, "__iter__") and not isinstance(value, str):
                    assert all(True for v in value if isinstance(v, (date, datetime)))
                    assert len(value) == 2
                    kwargs[arg] = f"{value[0].isoformat()}..{value[1].isoformat()}"
                    if not splittable_arg:
                        # We split only by the first met splittable argument
                        middle_value = value[0] + (value[1] - value[0]) / 2
                        if middle_value in value:
                            # When the middle value in itareble value, we can't use it
                            # to split by dates later
                            continue
                        splittable_arg = arg
                        splittable_value = value
                    continue
                assert isinstance(value, (date, datetime, str))

        inter_result = []  # type: Issues
        exception = RateLimitExceededException(0)
        for i in range(self.retries):
            try:
                logger.debug("Search issues, args=%s, kwargs=%s", args, kwargs)
                result = super().search_issues(*args, **kwargs)
                if result.totalCount == 1000 and splittable_arg:
                    # The hard limit is 1000. If it's splittable, then we make
                    # two subrequests requests with less time frames
                    logger.debug(
                        "The search result contain exactly 1000 results, "
                        "splitting %s=%s by middle point %s",
                        splittable_arg,
                        kwargs[splittable_arg],
                        middle_value,
                    )
                    kwargs[splittable_arg] = [splittable_value[0], middle_value]
                    inter_result.extend(self.search_issues(*args, **kwargs))
                    if isinstance(middle_value, date):
                        # When middle_value is a date, 2022-01-01..2022-01-03
                        # is split to 2022-01-01..2022-01-02 and
                        # 2022-01-02..2022-01-03, so we have results for
                        # 2022-01-02 twicely. We split it to
                        # 2022-01-01..2022-01-02 and 2022-01-03..2022-01-03.
                        # 2022-01-01..2022-01-02 aren't split, see splittable_arg
                        # definition above for kwargs.items
                        middle_value += timedelta(days=1)
                    kwargs[splittable_arg] = [middle_value, splittable_value[1]]
                    inter_result.extend(self.search_issues(*args, **kwargs))
                    return inter_result

                inter_result.extend(result)
                return inter_result
            except RateLimitExceededException as e:
                if i == self.retries - 1:
                    exception = e
                self.sleep_on_rate_limit()

        raise exception

    # pylint: enable=signature-differs
    def get_pulls_from_search(self, *args: Any, **kwargs: Any) -> PullRequests:
        """The search api returns actually issues, so we need to fetch PullRequests"""
        progress_func = kwargs.pop(
            "progress_func", lambda x: x
        )  # type: Callable[[Issues], Issues]
        threads = kwargs.pop("threads", DEFAULT_FETCH_THREADS)
        issues = self.search_issues(*args, **kwargs)

        # See https://github.com/PyGithub/PyGithub/issues/2202,
        # obj._rawData doesn't spend additional API requests
        # pylint: disable=protected-access
        repos = {}  # type: Dict[str, Repository]
        # Resolve the repository objects up front (cheap, usually a single repo)
        # so the parallel fetch below neither races to populate `repos` nor
        # triggers a lazy `issue.repository` API call from a worker thread.
        for issue in issues:
            repo_url = issue._rawData["repository_url"]
            if repo_url not in repos:
                repos[repo_url] = issue.repository

        def _fetch(issue: Issue) -> PullRequest:
            repo_url = issue._rawData["repository_url"]
            return self.get_pull_cached(repos[repo_url], issue.number, issue.updated_at)

        if threads <= 1 or len(issues) <= 1:
            return [_fetch(issue) for issue in progress_func(issues)]

        # Each `get_pull` is an independent request and each PR is cached to its
        # own file, so the fetches parallelize without shared mutable state.
        # `executor.map` preserves input order, keeping the result deterministic.
        with ThreadPoolExecutor(max_workers=threads) as executor:
            return list(progress_func(executor.map(_fetch, issues)))

    def get_release_pulls(self, repo_name: str) -> PullRequests:
        prs = self.get_pulls_from_search(
            query=f"type:pr repo:{repo_name} is:open",
            sort="created",
            order="asc",
            label="release",
        )
        # All PRs should belong to the repo_name
        prs = [pr for pr in prs if pr.head.repo.full_name == repo_name]
        # Ensure that the answer from GitHub is correct (we should always have some releases)
        assert prs
        return prs

    # The exact set of fields `PullRequestInfo` needs, shared by both GraphQL
    # entry points below.
    _GRAPHQL_PR_FIELDS = """
        number
        headRefName
        baseRefName
        merged
        body
        id
        url
        mergeCommit { oid }
        labels(first: 100) { nodes { name } }
        baseRepository { nameWithOwner }
        closingIssuesReferences(first: 50) {
            nodes { number repository { nameWithOwner } }
        }
    """

    @staticmethod
    def _pr_info_from_node(node: dict) -> PullRequestInfo:
        merge_commit = node["mergeCommit"] or {}
        repo = node["baseRepository"]["nameWithOwner"]
        # Keep only same-repo closing references: a `Closes #N` for an issue in
        # another repository would collide with this repo's issue numbering.
        closing = (node.get("closingIssuesReferences") or {}).get("nodes") or []
        closing_issue_numbers = [
            issue["number"]
            for issue in closing
            if (issue.get("repository") or {}).get("nameWithOwner") == repo
        ]
        return PullRequestInfo(
            number=node["number"],
            head_ref=node["headRefName"],
            base_ref=node["baseRefName"],
            label_names=[label["name"] for label in node["labels"]["nodes"]],
            merged=node["merged"],
            merge_commit_sha=merge_commit.get("oid"),
            body=node["body"] or "",
            node_id=node["id"],
            html_url=node["url"],
            repo=repo,
            closing_issue_numbers=closing_issue_numbers,
        )

    def _graphql(self, query: str, variables: dict) -> dict:
        # pylint: disable=protected-access
        requester = self._Github__requester  # type: ignore
        url = f"{requester.base_url}/graphql"
        for i in range(self.retries):
            try:
                _, data = requester.requestJsonAndCheck(
                    "POST", url, input={"query": query, "variables": variables}
                )
                # GraphQL reports query errors with HTTP 200 and an `errors` key,
                # so `requestJsonAndCheck` does not raise on them. A query can be
                # partially successful: e.g. a `pullRequest(number: N)` alias for
                # a number that does not exist in this repo returns `null` for
                # that field plus a NOT_FOUND error, while the other aliases
                # resolve. Use whatever `data` resolved; only raise when nothing
                # came back at all (malformed query, auth failure, ...).
                payload = data.get("data")
                errors = data.get("errors")
                if payload is None:
                    raise GithubException(
                        200, errors or "GraphQL query returned no data", None
                    )
                if errors:
                    logger.warning(
                        "GraphQL returned %s error(s); using partial data: %s",
                        len(errors),
                        errors[0].get("message", errors[0]),
                    )
                return payload
            except RateLimitExceededException:
                if i == self.retries - 1:
                    raise
                self.sleep_on_rate_limit()
        raise RuntimeError("Unreachable: GraphQL retry loop exited without result")

    def get_pulls_lightweight(
        self, query: str, merged: Optional[List[datetime]] = None
    ) -> List[PullRequestInfo]:
        """Fetch lightweight PR records matching a search `query` via GraphQL.

        A single GraphQL request returns up to 100 PRs, versus one REST request
        per PR in `get_pulls_from_search`. The GitHub search index caps any one
        query at 1000 results, so when `merged=[since, until]` is given and a
        sub-range would exceed that, the range is split in half and re-queried --
        mirroring `search_issues`. Results are de-duplicated by PR number.
        """
        gql = (
            "query($q:String!, $after:String) {"
            "  search(query:$q, type:ISSUE, first:100, after:$after) {"
            "    issueCount pageInfo { hasNextPage endCursor }"
            "    nodes { ... on PullRequest {" + self._GRAPHQL_PR_FIELDS + "} }"
            "  }"
            "}"
        )
        found = {}  # type: Dict[int, PullRequestInfo]

        def run(since: Optional[datetime], until: Optional[datetime]) -> None:
            q = query
            if since is not None:
                q = f"{query} merged:{since.isoformat()}..{until.isoformat()}"
            after = None
            first_page = True
            while True:
                search = self._graphql(gql, {"q": q, "after": after})["search"]
                if first_page and since is not None and search["issueCount"] > 1000:
                    middle = since + (until - since) / 2
                    if since < middle < until:
                        run(since, middle)
                        run(middle, until)
                        return
                first_page = False
                for node in search["nodes"]:
                    if node:  # search also returns plain issues -> empty fragment
                        info = self._pr_info_from_node(node)
                        found[info.number] = info
                if not search["pageInfo"]["hasNextPage"]:
                    return
                after = search["pageInfo"]["endCursor"]

        if merged is not None:
            assert len(merged) == 2
            run(merged[0], merged[1])
        else:
            run(None, None)
        return list(found.values())

    def get_pulls_lightweight_by_numbers(
        self, repo_name: str, numbers: List[int]
    ) -> Dict[int, PullRequestInfo]:
        """Fetch lightweight PR records for explicit PR numbers via GraphQL,
        batching many PRs per request using field aliases."""
        owner, name = repo_name.split("/")
        result = {}  # type: Dict[int, PullRequestInfo]
        batch_size = 50
        for start in range(0, len(numbers), batch_size):
            batch = numbers[start : start + batch_size]
            aliases = " ".join(
                f"pr{n}: pullRequest(number: {n}) {{{self._GRAPHQL_PR_FIELDS}}}"
                for n in batch
            )
            gql = (
                "query($owner:String!, $name:String!) {"
                f"  repository(owner:$owner, name:$name) {{ {aliases} }}"
                "}"
            )
            repository = self._graphql(gql, {"owner": owner, "name": name})[
                "repository"
            ]
            for n in batch:
                node = (repository or {}).get(f"pr{n}")
                if node:
                    result[n] = self._pr_info_from_node(node)
        return result

    def get_backport_merge_commits(
        self, repo_name: str, head_refs: List[str]
    ) -> Dict[str, Optional[str]]:
        """For each backport branch name, return the merge commit SHA of the
        merged PR opened from it (or ``None`` if there is none), batching many
        head refs per GraphQL request.

        Replaces one REST `get_pulls(head=...)` request per backport branch --
        which at a large lookback fans out into thousands of calls and trips
        GitHub's secondary rate limit -- with a handful of GraphQL queries.
        """
        owner, name = repo_name.split("/")
        result = {}  # type: Dict[str, Optional[str]]
        batch_size = 50
        for start in range(0, len(head_refs), batch_size):
            batch = head_refs[start : start + batch_size]
            aliases = " ".join(
                f'b{i}: pullRequests(headRefName: "{head}", states: MERGED, '
                "first: 1) { nodes { mergeCommit { oid } } }"
                for i, head in enumerate(batch)
            )
            gql = (
                "query($owner:String!, $name:String!) {"
                f"  repository(owner:$owner, name:$name) {{ {aliases} }}"
                "}"
            )
            repository = (
                self._graphql(gql, {"owner": owner, "name": name})["repository"] or {}
            )
            for i, head in enumerate(batch):
                node = repository.get(f"b{i}")
                oid = None
                if node and node["nodes"]:
                    merge_commit = node["nodes"][0].get("mergeCommit")
                    oid = merge_commit.get("oid") if merge_commit else None
                result[head] = oid
        return result

    def sleep_on_rate_limit(self) -> None:
        for limit, data in self.get_rate_limit().raw_data.items():
            if data["remaining"] == 0:
                sleep_time = data["reset"] - int(datetime.now().timestamp()) + 1
                if sleep_time > 0:
                    logger.warning(
                        "Faced rate limit for '%s' requests type, sleeping %s",
                        limit,
                        sleep_time,
                    )
                    sleep(sleep_time)
                return

    def get_pull_cached(
        self, repo: Repository, number: int, obj_updated_at: Optional[datetime] = None
    ) -> PullRequest:
        # clean any special symbol from the repo name, especially '/'
        repo_name = re.sub(r"\W", "_", repo.full_name)
        cache_file = self.cache_path / f"pr-{repo_name}-{number}.pickle"

        return self._get_repo_obj_cached(  # type: ignore
            repo, "get_pull", cache_file, number, obj_updated_at=obj_updated_at
        )

    def get_issue_cached(
        self, repo: Repository, number: int, obj_updated_at: Optional[datetime] = None
    ) -> Issue:
        # clean any special symbol from the repo name, especially '/'
        repo_name = re.sub(r"\W", "_", repo.full_name)
        cache_file = self.cache_path / f"issue-{repo_name}-{number}.pickle"

        return self._get_repo_obj_cached(  # type: ignore
            repo, "get_issue", cache_file, number, obj_updated_at=obj_updated_at
        )

    def _get_repo_obj_cached(
        self,
        repo: Repository,
        func: str,
        cache_file: Path,
        *args: Any,
        obj_updated_at: Optional[datetime] = None,
        **kwargs: Any,
    ) -> object:
        is_updated, cached_obj = self._is_cache_updated(cache_file, obj_updated_at)
        if is_updated and cached_obj is not None:
            logger.debug(
                "Getting object for `%s(%s, %s)` from cache",
                func,
                args,
                kwargs,
            )
            return cached_obj  # type: ignore

        logger.debug(
            "Getting object for `%s(%s, %s)` from API",
            func,
            args,
            kwargs,
        )

        for i in range(self.retries):
            try:
                obj = getattr(repo, func)(*args, **kwargs)
                break
            except RateLimitExceededException:
                if i == self.retries - 1:
                    raise
                self.sleep_on_rate_limit()
        logger.debug(
            "Caching object for `%s(%s, %s)` from API in %s",
            func,
            args,
            kwargs,
            cache_file,
        )
        if self.cache_path.is_dir():
            with open(cache_file, "wb") as prfd:
                self.dump(obj, prfd)  # type: ignore
        return obj

    def get_user_cached(
        self, login: str, obj_updated_at: Optional[datetime] = None
    ) -> Union[AuthenticatedUser, NamedUser]:
        cache_file = self.cache_path / f"user-{login}.pickle"

        is_updated, cached_user = self._is_cache_updated(cache_file, obj_updated_at)
        if is_updated and cached_user is not None:
            logger.debug("Getting user %s from cache", login)
            return cached_user  # type: ignore

        logger.debug("Getting PR #%s from API", login)
        for i in range(self.retries):
            try:
                user = self.get_user(login)
                break
            except RateLimitExceededException:
                if i == self.retries - 1:
                    raise
                self.sleep_on_rate_limit()
        logger.debug("Caching user %s from API in %s", login, cache_file)
        if self.cache_path.is_dir():
            with open(cache_file, "wb") as prfd:
                self.dump(user, prfd)  # type: ignore
        return user

    def _get_cached(self, path: Path):  # type: ignore
        with open(path, "rb") as ob_fd:
            return self.load(ob_fd)  # type: ignore

    # pylint: disable=protected-access
    @staticmethod
    def toggle_pr_draft(pr: PullRequest) -> None:
        """GH rest API does not provide a way to toggle the draft status for PR"""
        node_id = pr._rawData["node_id"]
        if pr.draft:
            action = (
                "mutation PullRequestReadyForReview($input:MarkPullRequestReadyForReviewInput!)"
                "{markPullRequestReadyForReview(input: $input){pullRequest{id}}}"
            )
        else:
            action = (
                "mutation ConvertPullRequestToDraft($input:ConvertPullRequestToDraftInput!)"
                "{convertPullRequestToDraft(input: $input){pullRequest{id}}}"
            )
        query = {
            "query": action,
            "variables": {"input": {"pullRequestId": node_id}},
        }
        url = f"{pr._requester.base_url}/graphql"
        _, data = pr._requester.requestJsonAndCheck("POST", url, input=query)
        if data.get("data"):
            pr._draft = pr._makeBoolAttribute(not pr.draft)

    # pylint: enable=protected-access

    def _is_cache_updated(
        self, cache_file: Path, obj_updated_at: Optional[datetime]
    ) -> Tuple[bool, object]:
        if not cache_file.is_file():
            return False, None
        cached_obj = self._get_cached(cache_file)
        # We don't want the cache_updated being always old,
        # for example in cases when the user is not updated for ages
        cache_updated = max(
            cache_file.stat().st_mtime, cached_obj.updated_at.timestamp()
        )
        if obj_updated_at is None:
            # When we don't know about the object is updated or not,
            # we update it once per hour
            obj_updated_at = datetime.now() - timedelta(hours=1)
        if obj_updated_at.timestamp() <= cache_updated:
            return True, cached_obj
        return False, cached_obj

    @property
    def cache_path(self) -> Path:
        return self._cache_path

    @cache_path.setter
    def cache_path(self, value: Union[str, Path]) -> None:
        self._cache_path = Path(value)
        if self._cache_path.exists():
            assert self._cache_path.is_dir()
        else:
            self._cache_path.mkdir(parents=True)

    @property
    def retries(self):
        if self._retries == 0:
            self._retries = 3
        return self._retries

    @retries.setter
    def retries(self, value: int) -> None:
        assert isinstance(value, int)
        self._retries = value

    # static methods not using pygithub
    @staticmethod
    def cancel_wf(repo, run_id, token, strict=False):
        headers = {"Authorization": f"token {token}"}
        url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/cancel"
        try:
            response = requests.post(url, headers=headers, timeout=10)
            response.raise_for_status()
            print(f"NOTE: Workflow [{run_id}] has been cancelled")
        except Exception as ex:
            print("ERROR: Got exception executing wf cancel request", ex)
            if strict:
                raise ex
