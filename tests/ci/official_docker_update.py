#!/usr/bin/env python

"""
1. Check if the update PRs exist in the upstream repos before work with it:
    https://github.com/docker-library/official-images/
    https://github.com/docker-library/docs/
2. update the fork repos to upstream:
    https://github.com/ClickHouse/docker-library-official-images
    https://github.com/ClickHouse/docker-library-docs
3. checkout all necessary repos:
    https://github.com/ClickHouse/docker-library
    https://github.com/ClickHouse/docker-library-official-images
    https://github.com/ClickHouse/docker-library-docs
"""

import argparse
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from pprint import pformat
from shutil import copy2, copytree, rmtree
from sys import executable
from typing import Any

from ci_buddy import CIBuddy
from env_helper import IS_CI, TEMP_PATH
from get_robot_token import get_best_robot_token
from git_helper import GIT_PREFIX, git_runner, is_shallow
from github_helper import GitHub, Repository
from official_docker import MAINTAINERS_HEADER, path_is_changed
from ssh import SSHKey

LIBRARY_BRANCH = "ClickHouse-docker-library"

temp_path = Path(TEMP_PATH)


def run(cmd: str, dry_run: bool = False, **kwargs: Any) -> str:
    if dry_run:
        logging.info("Dry run, would run the command: %s", cmd)
        return ""
    return git_runner(cmd, **kwargs)


def repo_merge_upstream(repo: Repository, branch: str) -> None:
    """A temporary hack to implement the following patch in out code base:
    https://github.com/PyGithub/PyGithub/pull/3175"""
    post_parameters = {"branch": branch}
    repo._requester.requestJsonAndCheck(  # pylint:disable=protected-access
        "POST", f"{repo.url}/merge-upstream", input=post_parameters
    )


@dataclass
class LibraryRepos:
    """A dataclass to store repositories to process"""

    gh: GitHub
    ldf: Repository
    images: Repository
    docs: Repository

    @staticmethod
    def get_repos(gh: GitHub, args: argparse.Namespace) -> "LibraryRepos":
        ldf = gh.get_repo(args.ldf_repo)
        images = gh.get_repo(args.images_repo)
        docs = gh.get_repo(args.docs_repo)
        return LibraryRepos.validate_repos(gh, ldf, images, docs)

    @staticmethod
    def validate_repos(
        gh: GitHub, ldf: Repository, images: Repository, docs: Repository
    ) -> "LibraryRepos":
        assert (
            ldf.owner.name == images.owner.name == docs.owner.name
        ), "all repositories must be in the same organization"
        return LibraryRepos(gh, ldf, images, docs)


def update_docs(repos: LibraryRepos, dry_run: bool = True) -> None:
    """
    - Check the parent repo don't have PRs opened from the fork
    - If has:
        - Checkout the branch
        - Check the documentation should be updated
        - Push changes if there are
        - Finish
    - Create the branch
    - Check the documentation should be updated:
        - Copy content of ./docker/server/README.src to the
            docker-library-docs/clickhouse
        - Find every file with `docker-official-library:off` in it, clean to the
            `docker-official-library:on`
        - Check if the repository content is changed
        - Finish, if it's the same
    - Push the changes
    - Create a PR
    """
    org = repos.ldf.owner.name

    ##############################################################
    # Check the fork of docker-library/docs is up-to-date or not #
    ##############################################################
    docs_dir = temp_path / repos.docs.name
    if docs_dir.is_dir():
        rmtree(docs_dir)

    if not dry_run:
        repo_merge_upstream(repos.docs, repos.docs.default_branch)

    run(f"{GIT_PREFIX} clone {repos.docs.ssh_url}", cwd=temp_path)

    pr_head = f"{org}:{LIBRARY_BRANCH}"
    open_images_prs = repos.docs.parent.get_pulls(state="open", head=pr_head)
    try:
        docs_pr = open_images_prs[0]
        logging.info("There's open PRs in upstream repo, will update it if needed")
        run(f"{GIT_PREFIX} checkout {LIBRARY_BRANCH}", cwd=docs_dir)
    except IndexError:
        docs_pr = None
        run(f"{GIT_PREFIX} checkout -B {LIBRARY_BRANCH} --no-track", cwd=docs_dir)

    # Copy and filter the docs sources
    ch_docs = docs_dir / "clickhouse"
    docs_src = Path(git_runner.cwd) / "docker/server/README.src"
    copytree(docs_src, ch_docs, dirs_exist_ok=True)
    start_filter = "<!-- docker-official-library:off -->"
    stop_filter = "<!-- docker-official-library:on -->"
    for md_file in docs_src.glob("*.md"):
        md_file = ch_docs / md_file.name
        original_content = md_file.read_text().splitlines()
        content = []
        filtering = False
        for line in original_content:
            # Filter out CI-related part from official docker
            if line == start_filter:
                filtering = True
                continue
            if line == stop_filter:
                filtering = False
                continue
            if not filtering:
                content.append(line)
        md_file.write_text("\n".join(content) + "\n")

    if not path_is_changed(ch_docs):
        logging.info("Nothing is changed in %s, finishing", repos.docs.parent.full_name)
        return

    logging.info("The diff:\n%s", run("git diff", cwd=docs_dir))
    if dry_run:
        logging.info(
            "The library/clickhouse file in %s is changed, would update the upstream",
            repos.images.parent.full_name,
        )

    run(
        f"{GIT_PREFIX} commit -m 'Update clickhouse according the the latest tags' "
        f"{ch_docs}",
        dry_run,
        cwd=docs_dir,
    )
    run(
        f"{GIT_PREFIX} push --set-upstream origin {LIBRARY_BRANCH}",
        dry_run,
        cwd=docs_dir,
    )

    if docs_pr is not None:
        logging.info("The branch for PR %s is updated", docs_pr.html_url)
        return

    if dry_run:
        logging.info("Dry running, would create a PR from %s", pr_head)
        return

    repos.docs.parent.create_pull(
        base=repos.docs.parent.default_branch,
        head=pr_head,
        title=f"Update {ch_docs.name} docs according to ClickHouse/ClickHouse",
        body=f"This is an automatic PR to update `{ch_docs.name}` docs.\n\n{MAINTAINERS_HEADER}",
    )


def update_library_images(repos: LibraryRepos, dry_run: bool = True) -> None:
    """
    - Check the parent repo don't have PRs opened from the fork
    - If has:
        - Checkout the branch
        - Check the LDF should be updated
        - Push changes if there are
    - Create directories in ClickHouse/docker-library via
        `tests/ci/official_docker.py generate-tree`
    - If there aren't changes:
        - Finish
    - Create a new LDF as `tests/ci/official_docker.py generate-ldf`
    - Update ClickHouse/docker-library-official-images to the upstream
    - Create a branch in ClickHouse/docker-library-official-images
    - Copy `ClickHouse/docker-library/clickhouse` to
        `ClickHouse/docker-library-official-images/clickhouse`
    - Commit, create a PR
    """
    org = repos.ldf.owner.name

    #############################################################
    # Update docker-library repository with Dockerfiles and LDF #
    #############################################################

    ldf_dir = temp_path / repos.ldf.name
    if ldf_dir.is_dir():
        rmtree(ldf_dir)

    run(f"{GIT_PREFIX} clone {repos.ldf.ssh_url}", cwd=temp_path)

    generate_tree_cmd = (
        f"{executable} tests/ci/official_docker.py generate-tree --build --fetch-tags "
        f"--directory {ldf_dir} --dockerfile-glob Dockerfile.ubuntu --image-type server"
    )

    logging.info(
        "Run command to check if the tree for LDF is changed: %s",
        f"{generate_tree_cmd} -vvv",
    )
    run(f"{generate_tree_cmd} -vvv")

    if dry_run:
        logging.info(
            "Dry running, continue running the script to check what would change"
        )
    elif not path_is_changed(ldf_dir / "*"):
        logging.info(
            "No changes in %s, finish updating %s",
            repos.ldf.full_name,
            repos.images.full_name,
        )
        return
    else:
        run(f"{generate_tree_cmd} --commit")

    generate_ldf_cmd = (
        f"{executable} tests/ci/official_docker.py generate-ldf -vvv "
        f"--directory {ldf_dir} --dockerfile-glob Dockerfile.ubuntu --image-type server"
    )
    if dry_run:
        generate_ldf_cmd = f"{generate_ldf_cmd} --no-check-changed"
    else:
        generate_ldf_cmd = f"{generate_ldf_cmd} --commit"

    run(generate_ldf_cmd)
    # Yes, right to the `main`
    run(f"{GIT_PREFIX} push", dry_run, cwd=ldf_dir)

    #############################
    # LDF repository is updated #
    #############################

    #########################################################################
    # Check the fork of docker-library/official-images is up-to-date or not #
    #########################################################################
    images_dir = temp_path / repos.images.name
    if images_dir.is_dir():
        rmtree(images_dir)

    if not dry_run:
        repo_merge_upstream(repos.images, repos.images.default_branch)

    run(f"{GIT_PREFIX} clone {repos.images.ssh_url}", cwd=temp_path)

    pr_head = f"{org}:{LIBRARY_BRANCH}"
    open_images_prs = repos.images.parent.get_pulls(state="open", head=pr_head)
    try:
        images_pr = open_images_prs[0]
        logging.info("There's open PRs in upstream repo, will update it if needed")
        run(f"{GIT_PREFIX} checkout {LIBRARY_BRANCH}", cwd=images_dir)
    except IndexError:
        images_pr = None
        run(f"{GIT_PREFIX} checkout -B {LIBRARY_BRANCH} --no-track", cwd=images_dir)

    ch_ldf = images_dir / "library/clickhouse"
    copy2(ldf_dir / "clickhouse", ch_ldf)
    if not path_is_changed(ch_ldf):
        logging.info(
            "Nothing is changed in %s, finishing", repos.images.parent.full_name
        )
        return

    logging.info("The diff:\n%s", run("git diff", cwd=images_dir))

    if dry_run:
        logging.info(
            "The %s file in %s is changed, would update the upstream",
            ch_ldf,
            repos.images.parent.full_name,
        )

    run(
        f"{GIT_PREFIX} commit -m 'Update clickhouse according the the latest tags' "
        f"{ch_ldf}",
        dry_run,
        cwd=images_dir,
    )
    run(
        f"{GIT_PREFIX} push --set-upstream origin {LIBRARY_BRANCH}",
        dry_run,
        cwd=images_dir,
    )

    if images_pr is not None:
        logging.info("The branch for PR %s is updated", images_pr.html_url)
        return

    if dry_run:
        logging.info("Dry running, would create a PR from %s", pr_head)
        return

    repos.images.parent.create_pull(
        base=repos.images.parent.default_branch,
        head=pr_head,
        title=f"Update {ch_ldf.name} to the latest state",
        body=f"This is an automatic PR to update `{ch_ldf.name}` according to the latest "
        f"releases.\n\n{MAINTAINERS_HEADER}",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to update all docker library repositories",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="set the script verbosity, could be used multiple",
    )
    parser.add_argument("--token", help="github token, if not set, used from smm")
    parser.add_argument(
        "--ldf-repo",
        default="ClickHouse/docker-library",
        help="repo with Library Definition File and Dockerfiles",
    )
    parser.add_argument(
        "--images-repo",
        default="ClickHouse/docker-library-official-images",
        help="fork repo of https://github.com/docker-library/official-images/",
    )
    parser.add_argument(
        "--docs-repo",
        default="ClickHouse/docker-library-docs",
        help="fork repo of https://github.com/docker-library/docs/",
    )

    parser.add_argument("--dry-run", action="store_true", help="do not create anything")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    log_levels = [logging.CRITICAL, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=log_levels[min(args.verbose, 3)])
    logging.debug("Arguments are %s", pformat(args.__dict__))
    token = args.token or get_best_robot_token()

    gh = GitHub(token, create_cache_dir=False)
    repos = LibraryRepos.get_repos(gh, args)
    try:
        update_library_images(repos, args.dry_run)
        update_docs(repos, args.dry_run)
    except Exception as e:
        logging.error("The process has finished with error: %s", e)
        if IS_CI:
            ci_buddy = CIBuddy()
            ci_buddy.post_job_error(
                f"The cherry-pick finished with errors: {e}",
                with_instance_info=True,
                with_wf_link=True,
                critical=True,
            )
        raise



if __name__ == "__main__":
    logging.getLogger().setLevel(level=logging.INFO)

    assert not is_shallow()
    if os.getenv("ROBOT_CLICKHOUSE_SSH_KEY", ""):
        with SSHKey("ROBOT_CLICKHOUSE_SSH_KEY"):
            main()
    else:
        main()
