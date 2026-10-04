#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: a `Replicated` database re-parses the column type from the formatted
# `CREATE`, and version 0 is not printed in the type name, so the version pinned on `ATTACH` differs
# from the one this test expects.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Rewriting the state version of a versioned aggregate function nested inside a wrapper type
# (on `ATTACH` and in `NativeWriter`) must not drop the wrapper's custom name: `Nested` must not
# collapse into `Array(Tuple(...))` and `SimpleAggregateFunction` must not collapse into its
# storage type. https://github.com/ClickHouse/ClickHouse/pull/112052

$CLICKHOUSE_CLIENT -m -q "
SET flatten_nested = 0;
DROP TABLE IF EXISTS nested_agg;
DROP TABLE IF EXISTS saf_agg;
CREATE TABLE nested_agg (n Nested(s AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO nested_agg SELECT [tuple(initializeAggregation('sumMapState', [1::UInt8], [2::UInt8]))];
CREATE TABLE saf_agg (x SimpleAggregateFunction(anyLast, AggregateFunction(0, sumMap, Array(UInt8), Array(UInt8)))) ENGINE = AggregatingMergeTree ORDER BY tuple();
INSERT INTO saf_agg SELECT initializeAggregation('sumMapState', [3::UInt8], [4::UInt8]);
"

# The type of the received block as written by `NativeWriter`, which rewrites the explicitly
# pinned version 0 to the version of the negotiated revision.
$CLICKHOUSE_CLIENT -q "SELECT n FROM nested_agg FORMAT TabSeparatedWithNamesAndTypes" | sed -n 2p
$CLICKHOUSE_CLIENT -q "SELECT x FROM saf_agg FORMAT TabSeparatedWithNamesAndTypes" | sed -n 2p

$CLICKHOUSE_CLIENT -m -q "
DETACH TABLE nested_agg;
ATTACH TABLE nested_agg;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'nested_agg';
SELECT finalizeAggregation((n[1]).s) FROM remote('127.0.0.2', currentDatabase(), nested_agg);
SELECT n.s FROM nested_agg FORMAT Null;
SELECT 'nested subcolumn ok';
DETACH TABLE saf_agg;
ATTACH TABLE saf_agg;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'saf_agg';
SELECT finalizeAggregation(x) FROM remote('127.0.0.2', currentDatabase(), saf_agg);
DROP TABLE nested_agg;
DROP TABLE saf_agg;
"
