#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Create a directory symlink cycle: a -> b, b -> a.
# Without cycle protection, `filesystem` would either iterate forever or
# accumulate unbounded paths (`a/a/a/...`) inside the cycle.
mkdir -p "${CLICKHOUSE_USER_FILES_UNIQUE}"
ln -s b "${CLICKHOUSE_USER_FILES_UNIQUE}/a"
ln -s a "${CLICKHOUSE_USER_FILES_UNIQUE}/b"

TEST_REL="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# Both symlinks must appear in the listing (as type=symlink), and the query
# must terminate quickly. We restrict to direct children to avoid surfacing
# any stray entries the cycle might produce.
$CLICKHOUSE_CLIENT --query "
    SELECT name, type, is_symlink
    FROM filesystem('${TEST_REL}')
    WHERE depth = 0 AND name IN ('a', 'b')
    ORDER BY name
"

# Also ensure that an unfiltered query terminates and returns a bounded number of rows.
$CLICKHOUSE_CLIENT --query "
    SELECT count() < 1000
    FROM filesystem('${TEST_REL}')
"

rm -rf "${CLICKHOUSE_USER_FILES_UNIQUE:?}"
