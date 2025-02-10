#!/usr/bin/env python3
import logging
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
    command = (
        f"rm -rf {dir} && mkdir {dir} && cd {dir} && git clone --quiet {repo} {dir}"
    )
    if commit:
        command += f" && git checkout --quiet {commit}"
    num_attempts = 10
    for attempt_no in range(1, num_attempts + 1):
        try:
            node.exec_in_container(["bash", "-c", command])
        except Exception as err:
            try_again = attempt_no < num_attempts
            what_next = "will try again" if try_again else "will stop"
            logging.warning(
                f"Attempt #{attempt_no} to clone repository {repo} failed. Error: {err}, {what_next}"
            )
            if not try_again:
                raise
            time.sleep(1)


def run_git_import(dir):
    command = f"cd {dir} && /usr/bin/clickhouse git-import 2>&1"
    return node.exec_in_container(["bash", "-c", command])


def create_tables():
    node.query(
        f"""
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
        f"""
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
        f"""
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
    node.query(f"DROP TABLE commits")
    node.query(f"DROP TABLE file_changes")
    node.query(f"DROP TABLE line_changes")


def test_git_import():
    repo = "https://github.com/githubtraining/hellogitworld.git"
    commit = "ef7bebf8bdb1919d947afe46ab4b2fb4278039b3"
    dir = "/tmp/hellogitworld"

    clone_git_repository(repo, dir, commit=commit)

    output = run_git_import(dir)

    create_tables()
    insert_into_tables(dir)

    assert output.count("\n") == 26
    assert node.query("SELECT count() FROM commits") == "24\n"
    assert node.query("SELECT count() FROM file_changes") == "35\n"
    assert node.query("SELECT count(), round(avg(indent), 1) FROM line_changes") == TSV(
        [[218, 1.1]]
    )

    drop_tables()
