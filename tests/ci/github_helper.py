#!/usr/bin/env python
"""Helper for GitHub API requests"""
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from os import path as p
from time import sleep
from typing import List, Optional, Tuple

import github
from github.GithubException import RateLimitExceededException
from github.Issue import Issue
from github.NamedUser import NamedUser
from github.PullRequest import PullRequest
from github.Repository import Repository

CACHE_PATH = p.join(p.dirname(p.realpath(__file__)), "gh_cache")

logger = logging.getLogger(__name__)

PullRequests = List[PullRequest]
Issues = List[Issue]


class GitHub(github.Github):
    def __init__(self, *args, **kwargs):
        # Define meta attribute
        self._cache_path = Path(CACHE_PATH)
        # And set Path
        super().__init__(*args, **kwargs)
        self._retries = 0

    # pylint: disable=signature-differs
    def search_issues(self, *args, **kwargs) -> Issues:  # type: ignore
        """Wrapper around search method with throttling and splitting by date.

        We split only by the first"""
        splittable = False
        for arg, value in kwargs.items():
            if arg in ["closed", "created", "merged", "updated"]:
                if hasattr(value, "__iter__") and not isinstance(value, str):
                    assert [True for v in value if isinstance(v, (date, datetime))]
                    assert len(value) == 2
                    kwargs[arg] = f"{value[0].isoformat()}..{value[1].isoformat()}"
                    if not splittable:
                        # We split only by the first met splittable argument
                        preserved_arg = arg
                        preserved_value = value
                        middle_value = value[0] + (value[1] - value[0]) / 2
                        splittable = middle_value not in value
                    continue
                assert isinstance(value, (date, datetime, str))

        inter_result = []  # type: Issues
        for i in range(self.retries):
            try:
                logger.debug("Search issues, args=%s, kwargs=%s", args, kwargs)
                result = super().search_issues(*args, **kwargs)
                if result.totalCount == 1000 and splittable:
                    # The hard limit is 1000. If it's splittable, then we make
                    # two subrequests requests with less time frames
                    logger.debug(
                        "The search result contain exactly 1000 results, "
                        "splitting %s=%s by middle point %s",
                        preserved_arg,
                        kwargs[preserved_arg],
                        middle_value,
                    )
                    kwargs[preserved_arg] = [preserved_value[0], middle_value]
                    inter_result.extend(self.search_issues(*args, **kwargs))
                    if isinstance(middle_value, date):
                        # When middle_value is a date, 2022-01-01..2022-01-03
                        # is split to 2022-01-01..2022-01-02 and
                        # 2022-01-02..2022-01-03, so we have results for
                        # 2022-01-02 twicely. We split it to
                        # 2022-01-01..2022-01-02 and 2022-01-03..2022-01-03.
                        # 2022-01-01..2022-01-02 aren't split, see splittable
                        middle_value += timedelta(days=1)
                    kwargs[preserved_arg] = [middle_value, preserved_value[1]]
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
    def get_pulls_from_search(self, *args, **kwargs) -> PullRequests:
        """The search api returns actually issues, so we need to fetch PullRequests"""
        issues = self.search_issues(*args, **kwargs)
        repos = {}
        prs = []  # type: PullRequests
        for issue in issues:
            # See https://github.com/PyGithub/PyGithub/issues/2202,
            # obj._rawData doesn't spend additional API requests
            # pylint: disable=protected-access
            repo_url = issue._rawData["repository_url"]  # type: ignore
            if repo_url not in repos:
                repos[repo_url] = issue.repository
            prs.append(
                self.get_pull_cached(repos[repo_url], issue.number, issue.updated_at)
            )
        return prs

    def sleep_on_rate_limit(self):
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
        cache_file = self.cache_path / f"pr-{number}.pickle"

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
        with open(cache_file, "wb") as prfd:
            self.dump(pr, prfd)  # type: ignore
        return pr

    def get_user_cached(
        self, login: str, obj_updated_at: Optional[datetime] = None
    ) -> NamedUser:
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
        with open(cache_file, "wb") as prfd:
            self.dump(user, prfd)  # type: ignore
        return user

    def _get_cached(self, path: Path):
        with open(path, "rb") as ob_fd:
            return self.load(ob_fd)  # type: ignore

    def _is_cache_updated(
        self, cache_file: Path, obj_updated_at: Optional[datetime]
    ) -> Tuple[bool, object]:
        cached_obj = self._get_cached(cache_file)
        # We don't want the cache_updated being always old,
        # for example in cases when the user is not updated for ages
        cache_updated = max(
            datetime.fromtimestamp(cache_file.stat().st_mtime), cached_obj.updated_at
        )
        if obj_updated_at is None:
            # When we don't know about the object is updated or not,
            # we update it once per hour
            obj_updated_at = datetime.now() - timedelta(hours=1)
        if obj_updated_at <= cache_updated:
            return True, cached_obj
        return False, cached_obj

    @property
    def cache_path(self):
        return self._cache_path

    @cache_path.setter
    def cache_path(self, value: str):
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
    def retries(self, value: int):
        self._retries = value
