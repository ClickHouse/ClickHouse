#!/usr/bin/env python
"""Helper for GitHub API requests"""
import logging
import re
from datetime import date, datetime, timedelta
from os import path as p
from pathlib import Path
from time import sleep
from typing import Any, Callable, List, Optional, Tuple, Union

import github
import requests

# explicit reimport
# pylint: disable=useless-import-alias
from github.AuthenticatedUser import AuthenticatedUser
from github.GithubException import (
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
        issues = self.search_issues(*args, **kwargs)
        repos = {}
        prs = []  # type: PullRequests
        for issue in progress_func(issues):
            # See https://github.com/PyGithub/PyGithub/issues/2202,
            # obj._rawData doesn't spend additional API requests
            # pylint: disable=protected-access
            repo_url = issue._rawData["repository_url"]
            if repo_url not in repos:
                repos[repo_url] = issue.repository
            prs.append(
                self.get_pull_cached(repos[repo_url], issue.number, issue.updated_at)
            )
        return prs

    def get_release_pulls(self, repo_name: str) -> PullRequests:
        return self.get_pulls_from_search(
            query=f"type:pr repo:{repo_name} is:open",
            sort="created",
            order="asc",
            label="release",
        )

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

        if cache_file.is_file():
            is_updated, cached_pr = self._is_cache_updated(cache_file, obj_updated_at)
            if is_updated:
                logger.debug("Getting PR #%s from cache", number)
                return cached_pr  # type: ignore
        logger.debug("Getting PR #%s from API", number)
        for i in range(self.retries):
            try:
                pr = repo.get_pull(number)
                break
            except RateLimitExceededException:
                if i == self.retries - 1:
                    raise
                self.sleep_on_rate_limit()
        logger.debug("Caching PR #%s from API in %s", number, cache_file)
        if self.cache_path.is_dir():
            with open(cache_file, "wb") as prfd:
                self.dump(pr, prfd)  # type: ignore
        return pr

    def get_user_cached(
        self, login: str, obj_updated_at: Optional[datetime] = None
    ) -> Union[AuthenticatedUser, NamedUser]:
        cache_file = self.cache_path / f"user-{login}.pickle"

        if cache_file.is_file():
            is_updated, cached_user = self._is_cache_updated(cache_file, obj_updated_at)
            if is_updated:
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
