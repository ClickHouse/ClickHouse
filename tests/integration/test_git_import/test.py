#!/usr/bin/env python3
import logging
import re
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def clone_git_repository(repo, dir, commit=None):
    command = f"rm -rf {dir} && mkdir {dir} && cd {dir} && GIT_TERMINAL_PROMPT=0 timeout 60 git clone --quiet {repo} {dir}"
    if commit:
        command += f" && git checkout --quiet {commit}"
    num_attempts = 10
    for attempt_no in range(1, num_attempts + 1):
        try:
            node.exec_in_container(["bash", "-c", command])
            return
        except Exception as err:
            try_again = attempt_no < num_attempts
            what_next = "will try again" if try_again else "will stop"
            logging.warning(
                f"Attempt #{attempt_no} to clone repository {repo} failed. Error: {err}, {what_next}"
            )
            if not try_again:
                raise
            time.sleep(1)


# `ClickHouseCluster` gives the whole container the running server's profraw merge pool
# (`LLVM_PROFILE_FILE=/debug/it-%c%4m.profraw`), which every `docker exec`-ed process inherits.
# A second writer cannot merge into a continuous-mode profile - it records its own writer's counter
# bias - so in a coverage build the profiling runtime writes
# `LLVM Profile Warning: Unable to merge profile data: source profile file is not compatible.` and
# two `File exists` errors to stderr. Give this one-shot process a file of its own: its coverage is
# still collected and it no longer touches the server's pool.
GIT_IMPORT_PROFILE_FILE = "/debug/git-import-%p.profraw"

# One progress line per processed commit, which `git-import` writes to stderr.
COMMIT_PROGRESS_RE = re.compile(r"^\d+%  \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}  [0-9a-f]{40}  ")


def run_git_import(dir):
    command = (
        f"cd {dir} && LLVM_PROFILE_FILE={GIT_IMPORT_PROFILE_FILE} "
        "/usr/bin/clickhouse git-import 2>&1"
    )
    return node.exec_in_container(["bash", "-c", command])


def create_tables():
    node.query(
        """
        CREATE TABLE commits
        (
            hash String,
            author LowCardinality(String),
            time DateTime,
            message String,
            files_added UInt32,
            files_deleted UInt32,
            files_renamed UInt32,
            files_modified UInt32,
            lines_added UInt32,
            lines_deleted UInt32,
            hunks_added UInt32,
            hunks_removed UInt32,
            hunks_changed UInt32
        ) ENGINE = MergeTree ORDER BY time
        """
    )

    node.query(
        """
        CREATE TABLE file_changes
        (
            change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
            path LowCardinality(String),
            old_path LowCardinality(String),
            file_extension LowCardinality(String),
            lines_added UInt32,
            lines_deleted UInt32,
            hunks_added UInt32,
            hunks_removed UInt32,
            hunks_changed UInt32,

            commit_hash String,
            author LowCardinality(String),
            time DateTime,
            commit_message String,
            commit_files_added UInt32,
            commit_files_deleted UInt32,
            commit_files_renamed UInt32,
            commit_files_modified UInt32,
            commit_lines_added UInt32,
            commit_lines_deleted UInt32,
            commit_hunks_added UInt32,
            commit_hunks_removed UInt32,
            commit_hunks_changed UInt32
        ) ENGINE = MergeTree ORDER BY time
        """
    )

    node.query(
        """
        CREATE TABLE line_changes
        (
            sign Int8,
            line_number_old UInt32,
            line_number_new UInt32,
            hunk_num UInt32,
            hunk_start_line_number_old UInt32,
            hunk_start_line_number_new UInt32,
            hunk_lines_added UInt32,
            hunk_lines_deleted UInt32,
            hunk_context LowCardinality(String),
            line LowCardinality(String),
            indent UInt8,
            line_type Enum('Empty' = 0, 'Comment' = 1, 'Punct' = 2, 'Code' = 3),

            prev_commit_hash String,
            prev_author LowCardinality(String),
            prev_time DateTime,

            file_change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
            path LowCardinality(String),
            old_path LowCardinality(String),
            file_extension LowCardinality(String),
            file_lines_added UInt32,
            file_lines_deleted UInt32,
            file_hunks_added UInt32,
            file_hunks_removed UInt32,
            file_hunks_changed UInt32,

            commit_hash String,
            author LowCardinality(String),
            time DateTime,
            commit_message String,
            commit_files_added UInt32,
            commit_files_deleted UInt32,
            commit_files_renamed UInt32,
            commit_files_modified UInt32,
            commit_lines_added UInt32,
            commit_lines_deleted UInt32,
            commit_hunks_added UInt32,
            commit_hunks_removed UInt32,
            commit_hunks_changed UInt32
        ) ENGINE = MergeTree ORDER BY time
        """
    )


def insert_into_tables(dir):
    command = f'/usr/bin/clickhouse client --query="INSERT INTO commits FORMAT TSV" < {dir}/commits.tsv'
    node.exec_in_container(["bash", "-c", command])

    command = f'/usr/bin/clickhouse client --query="INSERT INTO file_changes FORMAT TSV" < {dir}/file_changes.tsv'
    node.exec_in_container(["bash", "-c", command])

    command = f'/usr/bin/clickhouse client --query="INSERT INTO line_changes FORMAT TSV" < {dir}/line_changes.tsv'
    node.exec_in_container(["bash", "-c", command])


def drop_tables():
    node.query("DROP TABLE commits")
    node.query("DROP TABLE file_changes")
    node.query("DROP TABLE line_changes")


def test_git_import():
    repo = "https://github.com/githubtraining/hellogitworld.git"
    commit = "ef7bebf8bdb1919d947afe46ab4b2fb4278039b3"
    dir = "/tmp/hellogitworld"

    clone_git_repository(repo, dir, commit=commit)

    output = run_git_import(dir)

    create_tables()
    insert_into_tables(dir)

    # `git-import` prints the `git log` command it runs and the commit count to stdout, and one
    # progress line per commit to stderr, which the `2>&1` above merges into the same stream. Match
    # those shapes instead of counting every newline, so that a line written by anything other than
    # `git-import` - a profiling or sanitizer runtime, a loader warning - fails the thing it
    # actually broke rather than this count.
    lines = output.splitlines()
    assert "git log --reverse --no-merges --pretty=%H" in lines, output
    assert "Total 24 commits to process." in lines, output
    assert sum(1 for line in lines if COMMIT_PROGRESS_RE.match(line)) == 24, output
    assert node.query("SELECT count() FROM commits") == "24\n"
    assert node.query("SELECT count() FROM file_changes") == "35\n"
    assert node.query("SELECT count(), round(avg(indent), 1) FROM line_changes") == TSV(
        [[218, 1.1]]
    )

    drop_tables()
