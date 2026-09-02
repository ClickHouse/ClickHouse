DROP TABLE IF EXISTS table_view;
DROP TABLE IF EXISTS source_table;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE source_table (
  date Date,
  datetime DateTime,
  zoneId UInt64,
  test1 ALIAS zoneId == 1,
  test2 DEFAULT zoneId * 3,
  test3 MATERIALIZED zoneId * 5
) ENGINE = MergeTree(date, (date, zoneId), 8192);

CREATE MATERIALIZED VIEW table_view
ENGINE = MergeTree(date, (date, zoneId), 8192)
AS SELECT
  date,
  zoneId,
  test1,
  test2,
  test3
FROM source_table;

INSERT INTO source_table (date, datetime, zoneId) VALUES ('2018-12-10', '2018-12-10 23:59:59', 1);

SELECT * from table_view;

DROP TABLE IF EXISTS table_view;
DROP TABLE IF EXISTS source_table;
