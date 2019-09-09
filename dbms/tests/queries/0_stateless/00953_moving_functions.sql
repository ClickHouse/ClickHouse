DROP TABLE IF EXISTS moving_sum_num;
DROP TABLE IF EXISTS moving_sum_dec;

CREATE TABLE moving_sum_num (
  k String,
  dt DateTime,
  v UInt64
)
ENGINE = MergeTree ORDER BY (k, dt);

INSERT INTO moving_sum_num
  SELECT 'b' k, toDateTime('2001-02-03 00:00:00')+number as dt, number as v
  FROM system.numbers
  LIMIT 5
  UNION ALL
  SELECT 'a' k, toDateTime('2001-02-03 00:00:00')+number as dt, number as v
  FROM system.numbers
  LIMIT 5;

INSERT INTO moving_sum_num
  SELECT 'b' k, toDateTime('2001-02-03 01:00:00')+number as dt, 5+number as v
  FROM system.numbers
  LIMIT 5;

SELECT * FROM moving_sum_num ORDER BY k,dt FORMAT TabSeparatedWithNames;

SELECT k, groupArrayMovingSum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;
SELECT k, groupArrayMovingSum(3)(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;

SELECT k, groupArrayMovingAvg(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;
SELECT k, groupArrayMovingAvg(3)(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;

CREATE TABLE moving_sum_dec ENGINE = Memory AS
  SELECT k, dt, toDecimal64(v, 2) as v
  FROM moving_sum_num;

SELECT k, groupArrayMovingSum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;
SELECT k, groupArrayMovingSum(v) FROM (SELECT * FROM moving_sum_num ORDER BY k, dt) GROUP BY k ORDER BY k FORMAT TabSeparatedWithNamesAndTypes;

DROP TABLE moving_sum_dec;
DROP TABLE moving_sum_num;


