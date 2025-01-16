"""
1. Check if the update PRs exist in the upstream repos before work with it:
    https://github.com/docker-library/official-images/
    https://github.com/docker-library/docs/
1. update the fork repos to upstream:
    https://github.com/ClickHouse/docker-library-official-images
    https://github.com/ClickHouse/docker-library-docs
1. checkout all necessary repos:
    https://github.com/ClickHouse/docker-library
    https://github.com/ClickHouse/docker-library-official-images
    https://github.com/ClickHouse/docker-library-docs
"""

# import argparse
# import os
# import time
# from pathlib import Path
#
# from praktika.result import Result
# from praktika.utils import MetaClasses, Shell, Utils


def update_docs() -> None:
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


def update_library_images() -> None:
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
