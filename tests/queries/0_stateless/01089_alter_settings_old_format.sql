DROP TABLE IF EXISTS old_format_mt;

CREATE TABLE old_format_mt (
  event_date Date,
  key UInt64,
  value1 UInt64,
  value2 String
)
ENGINE = MergeTree(event_date, (key, value1), 8192);

ALTER TABLE old_format_mt MODIFY SETTING enable_mixed_granularity_parts = 1; --{serverError 36}

SELECT 1;

DROP TABLE IF EXISTS old_format_mt;
