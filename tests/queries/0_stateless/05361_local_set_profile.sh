#!/usr/bin/env bash
# `SET profile` in `clickhouse-local` must apply the settings of the profile to the queries after it.
# https://github.com/ClickHouse/ClickHouse/issues/123916

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -m -q "
CREATE SETTINGS PROFILE p SETTINGS max_block_size = 123, join_use_nulls = 1;
SET profile = 'p';
SELECT getSetting('max_block_size'), getSetting('join_use_nulls');
SELECT b FROM (SELECT 1 AS a) AS l LEFT JOIN (SELECT 2 AS a, 3 AS b) AS r USING (a);
-- a setting the client changed itself still applies on top of the profile
SET max_block_size = 456;
SELECT getSetting('max_block_size'), getSetting('join_use_nulls');
"

$CLICKHOUSE_LOCAL -m -q "
CREATE SETTINGS PROFILE r SETTINGS readonly = 1;
SET profile = 'r';
CREATE TABLE t (x UInt8) ENGINE = Memory; -- { serverError READONLY }
SELECT getSetting('readonly');
"
