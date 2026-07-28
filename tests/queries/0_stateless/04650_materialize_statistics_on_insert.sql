-- Tags: no-parallel-replicas

-- Checks that level-0 parts have materialized column statistics by default

-- Prevent randomization
SET use_statistics = 1;
SET async_insert = 0;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS tab_fact;
DROP TABLE IF EXISTS tab_dim;

CREATE TABLE tab_fact (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE tab_dim (id UInt64, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = 'basic, uniq_v2';

SYSTEM STOP MERGES tab_fact;
SYSTEM STOP MERGES tab_dim;

INSERT INTO tab_fact SELECT number, number * 2 FROM numbers(10000);
INSERT INTO tab_dim SELECT number, toString(number) FROM numbers(1000);

SELECT 'statistics materialized on insert', count(), min(length(statistics)) > 0
FROM system.parts_columns
WHERE database = currentDatabase()
    AND table IN ('tab_fact', 'tab_dim')
    AND active AND column IN ('id', 'v');

DROP TABLE tab_fact;
DROP TABLE tab_dim;
