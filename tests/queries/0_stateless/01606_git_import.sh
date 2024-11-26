#!/usr/bin/env bash
# Tags: no-debug, no-tsan, no-msan, no-ubsan, no-asan
# ^ because inserting a 50 MB file can be slow.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Clone some not too large repository and create a database from it.

cd $CLICKHOUSE_TMP || exit

# Protection for network errors
for _ in {1..10}; do
    rm -rf ./clickhouse-odbc
    git clone --quiet https://github.com/ClickHouse/clickhouse-odbc.git && pushd clickhouse-odbc 2> /dev/null > /dev/null && git checkout --quiet 5d84ec591c53cbb272593f024230a052690fdf69 && break
    sleep 1
done

${CLICKHOUSE_GIT_IMPORT} 2>&1 | wc -l

${CLICKHOUSE_CLIENT} --multiline --query "

DROP TABLE IF EXISTS commits;
DROP TABLE IF EXISTS file_changes;
DROP TABLE IF EXISTS line_changes;

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
) ENGINE = MergeTree ORDER BY time;

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
) ENGINE = MergeTree ORDER BY time;

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
) ENGINE = MergeTree ORDER BY time;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO commits FORMAT TSV" < commits.tsv
${CLICKHOUSE_CLIENT} --query "INSERT INTO file_changes FORMAT TSV" < file_changes.tsv
${CLICKHOUSE_CLIENT} --query "INSERT INTO line_changes FORMAT TSV" < line_changes.tsv

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM commits"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file_changes"
${CLICKHOUSE_CLIENT} --query "SELECT count(), round(avg(indent), 1) FROM line_changes"

${CLICKHOUSE_CLIENT} --multiline --query "
DROP TABLE commits;
DROP TABLE file_changes;
DROP TABLE line_changes;
"
