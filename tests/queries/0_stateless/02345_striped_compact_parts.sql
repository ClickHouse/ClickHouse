-- Tags: no-random-settings

DROP TABLE IF EXISTS t_striped;

CREATE TABLE t_striped (id UInt32, s String, arr Array(UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', compact_parts_max_granules_to_buffer = 16;

SYSTEM STOP MERGES t_striped;

INSERT INTO t_striped select number % 10, randomPrintableASCII(rand() % 30), range(number % 30) from numbers(100000);
INSERT INTO t_striped select number % 10, randomPrintableASCII(rand() % 30), range(number % 30) from numbers(100000);
INSERT INTO t_striped select number % 10, randomPrintableASCII(rand() % 30), range(number % 30) from numbers(100000);

SELECT count() FROM t_striped WHERE NOT ignore(*);
SYSTEM START MERGES t_striped;

OPTIMIZE TABLE t_striped FINAL;

SELECT count() FROM t_striped WHERE NOT ignore(*);
SELECT count() FROM t_striped WHERE id = 1 OR id = 7 AND NOT ignore(*) SETTINGS max_rows_to_read = 85000;

DROP TABLE t_striped;
