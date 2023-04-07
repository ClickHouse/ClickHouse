#!/usr/bin/env bash
set -e
# The script to update user-guides documentation repo
# https://github.com/ClickHouse/clickhouse-docs

WORKDIR=$(dirname "$0")
WORKDIR=$(readlink -f "${WORKDIR}")
cd "$WORKDIR"

UPDATE_PERIOD_HOURS="${1:-24}"  # By default update once per 24 hours; 0 means "always update"

if [ ! -d "clickhouse-docs" ]; then
    echo "There's no clickhouse-docs/ dir, run get-clickhouse-docs.sh first to clone the repo"
    exit 1
fi

# Do not update it too often
LAST_FETCH_TS=$(stat -c %Y clickhouse-docs/.git/FETCH_HEAD 2>/dev/null || echo 0)
CURRENT_TS=$(date +%s)
HOURS_SINCE_LAST_FETCH=$(( (CURRENT_TS - LAST_FETCH_TS) / 60 / 60 ))

if [ "$HOURS_SINCE_LAST_FETCH" -lt "$UPDATE_PERIOD_HOURS" ]; then
    exit 0;
fi

echo "Updating clickhouse-docs..."
git -C clickhouse-docs pull
