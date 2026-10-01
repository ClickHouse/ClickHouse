#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: a background mutation selects its analyzer from the background context, so a
# session `enable_analyzer` cannot reach the `ALTER ... UPDATE` arms.

# The table name in the first argument of `joinGet` in a mutation is qualified with the database of
# the updated table, under the same `WITH` scoping as the other carriers: a mutation is executed
# later in a context whose current database is unrelated to the query's, so a bare name followed
# whoever ran it. An alias of an enclosing `SELECT` is not a table name, and a body which stops
# inheriting (`enable_global_with_statement = 0`) sees the table again.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB1=${CLICKHOUSE_DATABASE_1}

${CLICKHOUSE_CLIENT} <<EOF
SET enable_lightweight_update = 1;
CREATE DATABASE ${DB1};

-- 7 = the updated table's database, 1 = the session database, 77 = the table the alias names.
CREATE TABLE src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO src VALUES (1, 1);
CREATE TABLE ${DB1}.src (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO ${DB1}.src VALUES (1, 7);
CREATE TABLE ${DB1}.join_source (k UInt64, v UInt64) ENGINE = Join(ANY, LEFT, k);
INSERT INTO ${DB1}.join_source VALUES (1, 77);

CREATE TABLE ${DB1}.t (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${DB1}.t VALUES (1, 0);
CREATE TABLE ${DB1}.u (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id
    SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO ${DB1}.u VALUES (1, 0);

ALTER TABLE ${DB1}.t UPDATE v = joinGet(src, 'v', id) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'alter bare', v FROM ${DB1}.t;
ALTER TABLE ${DB1}.t UPDATE v = 0 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE ${DB1}.t UPDATE v = joinGet('src', 'v', id) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'alter string', v FROM ${DB1}.t;
ALTER TABLE ${DB1}.t UPDATE v = 0 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE ${DB1}.t
    UPDATE v = (WITH '${DB1}.join_source' AS src SELECT (SELECT joinGet(src, 'v', toUInt64(1)) SETTINGS enable_global_with_statement = 0))
    WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'alter out of scope', v FROM ${DB1}.t;
ALTER TABLE ${DB1}.t UPDATE v = 0 WHERE 1 SETTINGS mutations_sync = 2;
ALTER TABLE ${DB1}.t
    UPDATE v = (WITH '${DB1}.join_source' AS src SELECT (SELECT joinGet(src, 'v', toUInt64(1))))
    WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'alter in scope', v FROM ${DB1}.t;

UPDATE ${DB1}.u SET v = joinGet(src, 'v', id) WHERE 1;
SELECT 'update bare', v FROM ${DB1}.u;
UPDATE ${DB1}.u
    SET v = (WITH '${DB1}.join_source' AS src SELECT (SELECT joinGet(src, 'v', toUInt64(1)) SETTINGS enable_global_with_statement = 0))
    WHERE 1;
SELECT 'update out of scope', v FROM ${DB1}.u;
UPDATE ${DB1}.u
    SET v = (WITH '${DB1}.join_source' AS src SELECT (SELECT joinGet(src, 'v', toUInt64(1))))
    WHERE 1;
SELECT 'update in scope', v FROM ${DB1}.u;
UPDATE ${DB1}.u SET v = 0 WHERE joinGet(src, 'v', id) = 7;
SELECT 'update predicate', v FROM ${DB1}.u;

-- The command stored for the mutation carries the qualified name.
SELECT 'stored', countIf(position(command, '${DB1}.src') > 0) FROM system.mutations
WHERE database = '${DB1}' AND table = 't' AND position(command, 'joinGet') > 0 AND position(command, 'join_source') = 0;

DROP DATABASE ${DB1};
EOF
