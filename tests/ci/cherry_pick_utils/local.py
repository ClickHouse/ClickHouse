# -*- coding: utf-8 -*-

import functools
import logging
import os
import re

import git


class RepositoryBase:
    def __init__(self, repo_path):

        self._repo = git.Repo(repo_path, search_parent_directories=(not repo_path))

        # comparator of commits
        def cmp(x, y):
            if str(x) == str(y):
                return 0
            if self._repo.is_ancestor(x, y):
                return -1
            else:
                return 1

        self.comparator = functools.cmp_to_key(cmp)

    def iterate(self, begin, end):
        rev_range = f"{begin}...{end}"
        for commit in self._repo.iter_commits(rev_range, first_parent=True):
            yield commit


class Repository(RepositoryBase):
    def __init__(self, repo_path, remote_name, default_branch_name):
        super().__init__(repo_path)
        self._remote = self._repo.remotes[remote_name]
        self._remote.fetch()
        self._default = self._remote.refs[default_branch_name]

    def get_head_commit(self):
        return self._repo.commit(self._default)

    def get_release_branches(self):
        """
        Returns sorted list of tuples:
         * remote branch (git.refs.remote.RemoteReference),
         * base commit (git.Commit),
         * head (git.Commit)).
        List is sorted by commits in ascending order.
        """
        release_branches = []

        RE_RELEASE_BRANCH_REF = re.compile(r"^refs/remotes/.+/\d+\.\d+$")

        for branch in [
            r for r in self._remote.refs if RE_RELEASE_BRANCH_REF.match(r.path)
        ]:
            base = self._repo.merge_base(self._default, self._repo.commit(branch))
            if not base:
                logging.info(
                    "Branch %s is not based on branch %s. Ignoring.",
                    branch.path,
                    self._default,
                )
            elif len(base) > 1:
                logging.info(
                    "Branch %s has more than one base commit. Ignoring.", branch.path
                )
            else:
                release_branches.append((os.path.basename(branch.name), base[0]))

        return sorted(release_branches, key=lambda x: self.comparator(x[1]))


class BareRepository(RepositoryBase):
    def __init__(self, repo_path, default_branch_name):
        super().__init__(repo_path)
        self._default = self._repo.branches[default_branch_name]

    def get_release_branches(self):
        """
        Returns sorted list of tuples:
         * branch (git.refs.head?),
         * base commit (git.Commit),
         * head (git.Commit)).
        List is sorted by commits in ascending order.
        """
        release_branches = []

        RE_RELEASE_BRANCH_REF = re.compile(r"^refs/heads/\d+\.\d+$")

        for branch in [
            r for r in self._repo.branches if RE_RELEASE_BRANCH_REF.match(r.path)
        ]:
            base = self._repo.merge_base(self._default, self._repo.commit(branch))
            if not base:
                logging.info(
                    "Branch %s is not based on branch %s. Ignoring.",
                    branch.path,
                    self._default,
                )
            elif len(base) > 1:
                logging.info(
                    "Branch %s has more than one base commit. Ignoring.", branch.path
                )
            else:
                release_branches.append((os.path.basename(branch.name), base[0]))

        return sorted(release_branches, key=lambda x: self.comparator(x[1]))
