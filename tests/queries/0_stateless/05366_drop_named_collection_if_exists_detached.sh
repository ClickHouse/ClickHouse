#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: named collections are stored in SQL, which the fast test does not set up.
# no-replicated-database: like the other named collection dependency tests, the detached table is local.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `DROP NAMED COLLECTION IF EXISTS` of a collection that is already gone is a no-op, even when a
# detached table still records a dependency on it.

NC="nc_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
CREATE TABLE t (x UInt32) ENGINE = URL(${NC});
DETACH TABLE t;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
SET check_named_collection_dependencies = 0;
DROP NAMED COLLECTION ${NC};
SET check_named_collection_dependencies = 1;
DROP NAMED COLLECTION IF EXISTS ${NC};
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_DOESNT_EXIST, NAMED_COLLECTION_IS_USED }
SELECT count() FROM system.named_collections WHERE name = '${NC}';
"

# Restore the collection so the detached table can be attached and dropped.
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${NC} AS url = 'http://localhost:8123', format = 'CSV';
ATTACH TABLE t;
DROP TABLE t;
DROP NAMED COLLECTION ${NC};
"
