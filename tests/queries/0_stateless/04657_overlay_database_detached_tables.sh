#!/usr/bin/env bash
# A read-only `Overlay` facade has no detached tables of its own, and a scan of the detached
# tables must not fail because a facade is present: `system.detached_tables` opens the
# detached-table iterator of every scanned database, and `IDatabase` answers `NOT_IMPLEMENTED` by
# default. The scan is restricted to the databases of this test because the same default makes an
# unrestricted scan fail on unrelated databases of concurrently running tests (e.g. a `Filesystem`
# database) — pre-existing behavior that is not specific to `Overlay`.
# A table detached in a source database is also not reported under the facade: `ATTACH` and `DETACH`
# through the facade are rejected, so a detached name is not part of the facade's namespace.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB_OVL="db_ovl_${CLICKHOUSE_DATABASE}"
DB_SRC="db_src_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP DATABASE IF EXISTS ${DB_OVL};
DROP DATABASE IF EXISTS ${DB_SRC};

CREATE DATABASE ${DB_SRC};
CREATE TABLE ${DB_SRC}.attached (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE ${DB_SRC}.gone (x UInt64) ENGINE = MergeTree ORDER BY x;
DETACH TABLE ${DB_SRC}.gone PERMANENTLY;

CREATE DATABASE ${DB_OVL} ENGINE = Overlay('${DB_SRC}');

SELECT 'a scan of the detached tables works with a facade present';
SELECT count() >= 1 FROM system.detached_tables WHERE database IN ('${DB_SRC}', '${DB_OVL}');

SELECT 'the detached table of the source is reported for the source only';
SELECT if(database = '${DB_SRC}', 'source', 'facade') AS which, table
FROM system.detached_tables WHERE database IN ('${DB_SRC}', '${DB_OVL}') ORDER BY which, table;

SELECT 'the facade reports no detached tables even when asked about it directly';
SELECT count() FROM system.detached_tables WHERE database = '${DB_OVL}';

SELECT 'the attached table is still visible through the facade';
SELECT name FROM system.tables WHERE database = '${DB_OVL}' ORDER BY name;

DROP DATABASE ${DB_OVL};
DROP DATABASE ${DB_SRC};
"
