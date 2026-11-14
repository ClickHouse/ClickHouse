#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL UDFs are server-global, so scope the name to the per-test database to stay parallel-safe.
UDF="${CLICKHOUSE_DATABASE}_udf_with_alias"

$CLICKHOUSE_CLIENT -m -q "
SET enable_analyzer = 1;

DROP FUNCTION IF EXISTS ${UDF};
CREATE FUNCTION ${UDF} AS event -> array(length(event) AS l1, (l1 + l1) AS l2);

DROP TABLE IF EXISTS event_test;
CREATE TABLE event_test
(
    id UInt64,
    event String,
    x__event Array(UInt64) ALIAS ${UDF}(event)
)
ENGINE = MergeTree
ORDER BY id;

-- The UDF is substituted at CREATE TABLE, so the stored expression keeps the inline aliases that trigger the bug.
SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table = 'event_test' AND name = 'x__event';

INSERT INTO event_test VALUES (1, 'ab'), (2, 'abcd');

SELECT 'before drop', id, event, x__event FROM event_test ORDER BY id;

ALTER TABLE event_test ADD COLUMN time DateTime64(3) ALIAS toDateTime64(JSONExtract(event, 'time', 'String'), 3);
SELECT 'time added', count() FROM system.columns
WHERE database = currentDatabase() AND table = 'event_test' AND name = 'time';

-- Used to throw UNKNOWN_IDENTIFIER while scanning the x__event dependency.
ALTER TABLE event_test DROP COLUMN time;
SELECT 'time dropped', count() FROM system.columns
WHERE database = currentDatabase() AND table = 'event_test' AND name = 'time';

SELECT 'after drop', id, event, x__event FROM event_test ORDER BY id;

-- 'event' is not in the sorting key, so this rejection comes from the x__event ALIAS dependency.
ALTER TABLE event_test DROP COLUMN event; -- { serverError ILLEGAL_COLUMN }

DROP TABLE event_test;
DROP FUNCTION ${UDF};
"
