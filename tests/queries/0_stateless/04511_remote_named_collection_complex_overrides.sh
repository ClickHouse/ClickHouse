#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The named-collection form of the `remote` table function and the `Remote` storage engine must keep
# the collection's own `db`/`table` values when a complex (non-literal) override such as
# `sharding_key = rand()` is present (it used to fall back to the hard-coded `system` database), and
# must not require a `table` key when the target is a table function (`database = merge(...)`).
# https://github.com/ClickHouse/ClickHouse/pull/106189
#
# Named collections are server-global, so the collection names are scoped to the (unique) test
# database to avoid collisions across concurrent runs.
NC_FULL="nc_remote_full_${CLICKHOUSE_DATABASE}"
NC_BARE="nc_remote_bare_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE IF EXISTS nc_target;
CREATE TABLE nc_target (x UInt8) ENGINE = Memory;
INSERT INTO nc_target VALUES (1), (2), (3);

DROP NAMED COLLECTION IF EXISTS ${NC_FULL};
DROP NAMED COLLECTION IF EXISTS ${NC_BARE};

-- A collection carrying its own \`db\` and \`table\`.
CREATE NAMED COLLECTION ${NC_FULL} AS host = '127.0.0.1', db = '${CLICKHOUSE_DATABASE}', \`table\` = 'nc_target';
-- A collection carrying only the address: the target comes from the override.
CREATE NAMED COLLECTION ${NC_BARE} AS host = '127.0.0.1';

-- A complex override must not discard the collection's \`db\` and \`table\`.
SELECT count() FROM remote(${NC_FULL}, sharding_key = rand());

-- A table-function target needs no dummy \`table\` key in the collection.
SELECT count() FROM remote(${NC_BARE}, database = merge(currentDatabase(), '^nc_target\$'));

-- The \`Remote\` engine reuses the same parser, so both forms must work for it as well.
CREATE TABLE t_remote_nc ENGINE = Remote(${NC_FULL}, sharding_key = rand());
SELECT count() FROM t_remote_nc;
DROP TABLE t_remote_nc;

CREATE TABLE t_remote_nc_tf ENGINE = Remote(${NC_BARE}, database = merge(currentDatabase(), '^nc_target\$'));
SELECT count() FROM t_remote_nc_tf;
DROP TABLE t_remote_nc_tf;

DROP NAMED COLLECTION ${NC_FULL};
DROP NAMED COLLECTION ${NC_BARE};
DROP TABLE nc_target;
"
