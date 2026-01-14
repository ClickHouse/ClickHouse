#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-ordinary-database, no-encrypted-storage, no-shared-merge-tree

# Test case for the bug where assertHasValidVersionMetadata() fails
# for rolled-back parts when the table is destroyed during ATTACH AS REPLICATED.
# The issue was that rolled-back parts have creation_csn == RolledBackCSN,
# and the assertion tried to read the version metadata file which might be
# missing or inconsistent for such parts.
#
# Note: After ATTACH AS REPLICATED, rolled-back parts become visible again (count=1)
# because the table is reloaded from disk without transaction metadata context.
# This is expected behavior for this conversion operation.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -n -q "
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY tuple();

    BEGIN TRANSACTION;
    INSERT INTO TABLE t0 (c0) VALUES (1);
    ROLLBACK;

    DETACH TABLE t0;
    ATTACH TABLE t0 AS REPLICATED;

    SELECT COUNT(*) FROM t0;

    DETACH TABLE t0 SYNC;
    ATTACH TABLE t0 AS NOT REPLICATED;

    DROP TABLE t0;
"
