-- Test: _row_exists is a hidden system column — never exposed to users.
-- It must not appear in system.columns or SELECT *.

DROP TABLE IF EXISTS t_lwd_hidden;

CREATE TABLE t_lwd_hidden (a UInt32, b String)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lwd_hidden SELECT number, toString(number) FROM numbers(100);
OPTIMIZE TABLE t_lwd_hidden FINAL;

-- Before LWD: exactly 2 user columns, no _row_exists
SELECT count() = 2
FROM system.columns
WHERE table = 't_lwd_hidden' AND database = currentDatabase();

SELECT count() = 0
FROM system.columns
WHERE table = 't_lwd_hidden' AND database = currentDatabase() AND name = '_row_exists';

SET lightweight_deletes_sync = 2;
DELETE FROM t_lwd_hidden WHERE a < 10;

-- After LWD: user column count unchanged (still 2)
SELECT count() = 2
FROM system.columns
WHERE table = 't_lwd_hidden' AND database = currentDatabase();

-- _row_exists still absent from system.columns
SELECT count() = 0
FROM system.columns
WHERE table = 't_lwd_hidden' AND database = currentDatabase() AND name = '_row_exists';

-- SELECT * must not include _row_exists — verify via explicit column list from system.columns
SELECT arraySort(groupArray(name)) FROM system.columns
WHERE table = 't_lwd_hidden' AND database = currentDatabase();

-- After merge: still hidden, results still correct
OPTIMIZE TABLE t_lwd_hidden FINAL;

SELECT count() = 0
FROM system.columns
WHERE table = 't_lwd_hidden' AND database = currentDatabase() AND name = '_row_exists';

SELECT count() = 90 FROM t_lwd_hidden;

DROP TABLE t_lwd_hidden;
