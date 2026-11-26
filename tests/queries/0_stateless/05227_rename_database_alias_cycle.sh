#!/usr/bin/env bash
# https://github.com/ClickHouse/ClickHouse/issues/116906
# `RENAME DATABASE` used to move an `Alias` table into its own target namespace without
# re-validating the stored engine arguments, leaving `db.t = Alias(db, t)` behind. That cycle in the
# server-wide table dependency graph then made every unrelated DDL statement that adds a dependency
# edge fail with `INFINITE_LOOP`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The database names are derived from the per-test database, so concurrent runs of this test
# (the flaky check runs it several times in parallel) do not collide.
DB="${CLICKHOUSE_DATABASE}"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -nm -q "
        DROP DATABASE IF EXISTS ${DB}_from;
        DROP DATABASE IF EXISTS ${DB}_to;
        DROP DATABASE IF EXISTS ${DB}_pair;
        DROP DATABASE IF EXISTS ${DB}_paired;
        DROP DATABASE IF EXISTS ${DB}_cross;
        DROP DATABASE IF EXISTS ${DB}_cross_to;
        DROP DATABASE IF EXISTS ${DB}_keep;
        DROP DATABASE IF EXISTS ${DB}_ok;
        DROP DATABASE IF EXISTS ${DB}_ok_renamed;
    " 2>/dev/null ||:
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -nm -q "
    CREATE DATABASE ${DB}_from;

    -- The target does not exist yet, so the self-reference check at CREATE passes vacuously.
    CREATE TABLE ${DB}_from.t ENGINE = Alias('${DB}_to', 't');

    RENAME DATABASE ${DB}_from TO ${DB}_to; -- { serverError INFINITE_LOOP }

    -- The database is untouched and unrelated DDL still works.
    SELECT name, engine FROM system.tables WHERE database = '${DB}_from' ORDER BY name;

    CREATE TABLE t_base (x Int32) ENGINE = MergeTree ORDER BY x;
    CREATE VIEW v AS SELECT x FROM t_base;
    SELECT count() FROM v;

    -- A two-table cycle is rejected as well.
    CREATE DATABASE ${DB}_pair;
    CREATE TABLE ${DB}_pair.a ENGINE = Alias('${DB}_paired', 'b');
    CREATE TABLE ${DB}_pair.b ENGINE = Alias('${DB}_paired', 'a');
    RENAME DATABASE ${DB}_pair TO ${DB}_paired; -- { serverError INFINITE_LOOP }

    -- A cycle that runs through a third database and does not involve Alias at all is rejected too:
    -- before the rename ${DB}_cross_to.d is only a placeholder node in the dependency graph, and the
    -- rename merges the renamed dictionary into it.
    CREATE DATABASE ${DB}_keep;
    CREATE DICTIONARY ${DB}_keep.u (x UInt64, y UInt64) PRIMARY KEY x
    SOURCE(CLICKHOUSE(db '${DB}_cross_to' table 'd')) LAYOUT(FLAT()) LIFETIME(0);
    CREATE DATABASE ${DB}_cross;
    CREATE DICTIONARY ${DB}_cross.d (x UInt64, y UInt64) PRIMARY KEY x
    SOURCE(CLICKHOUSE(db '${DB}_keep' table 'u')) LAYOUT(FLAT()) LIFETIME(0);
    RENAME DATABASE ${DB}_cross TO ${DB}_cross_to; -- { serverError INFINITE_LOOP }

    -- A rename that does not create a cycle still works.
    CREATE DATABASE ${DB}_ok;
    CREATE TABLE ${DB}_ok.t ENGINE = Alias(currentDatabase(), 't_base');
    RENAME DATABASE ${DB}_ok TO ${DB}_ok_renamed;
    SELECT count() FROM ${DB}_ok_renamed.t;

    DROP DATABASE ${DB}_from;
    -- ${DB}_cross.d depends on ${DB}_keep.u, so the dependent database goes first.
    DROP DATABASE ${DB}_cross;
    DROP DATABASE ${DB}_keep;
    DROP DATABASE ${DB}_pair;
    DROP DATABASE ${DB}_ok_renamed;
    DROP VIEW v;
    DROP TABLE t_base;
"
