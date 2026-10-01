#!/usr/bin/env bash
# A table that an older server stored with an alias in PRIMARY KEY or ORDER BY still loads:
# only a new definition is refused (05301_storage_key_alias_not_allowed).
#
# Such a definition can no longer be written through SQL, so the stored metadata of a table
# created without the aliases is edited into the form the older server wrote.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (c0 Int64, c1 Int64) ENGINE = MergeTree PRIMARY KEY c0 ORDER BY (c0, c1);
INSERT INTO db.t VALUES (1, 1), (2, 2);
"

metadata_file=$(grep -rl 'ENGINE = MergeTree' "${WORKING_DIR}" --include='t.sql')
sed -i 's/PRIMARY KEY c0/PRIMARY KEY (c0 AS a)/; s/ORDER BY (c0, c1)/ORDER BY (c0 AS x, c1)/' "${metadata_file}"
# Without this the load below would run against an unmodified definition, i.e. assert nothing.
grep -c -F -e 'PRIMARY KEY (c0 AS a)' -e 'ORDER BY (c0 AS x, c1)' "${metadata_file}"

$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
SELECT count() FROM db.t;
DETACH TABLE db.t;
ATTACH TABLE db.t;
SELECT sorting_key, primary_key FROM system.tables WHERE database = 'db' AND name = 't';
"

rm -rf "${WORKING_DIR}"
