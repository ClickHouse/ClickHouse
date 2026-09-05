-- The fast path of `MATERIALIZE TTL` proves its constant shift against the rows-TTL expression stored in
-- the part's `ttl.txt`, which means it parses that expression back. So the expression must be recorded in
-- a form that round-trips: with `getColumnName` (which writes identifiers raw) a column name that needs
-- quoting produced `plus(create time, toIntervalDay(300))`, which does not parse - and since an
-- unparseable fingerprint is escalated rather than swallowed, `MODIFY TTL` on such a table would fail.

SET alter_sync = 2;

DROP TABLE IF EXISTS t_ttl_quoted;

-- `min_bytes_for_full_part_storage` is pinned because the in-place rewrite of `ttl.txt` needs the part's
-- files to be stored separately; a packed part takes the regular rewrite.
CREATE TABLE t_ttl_quoted (id UInt32, `create time` DateTime('UTC')) ENGINE = MergeTree ORDER BY id
    TTL `create time` + INTERVAL 300 DAY
    SETTINGS min_bytes_for_full_part_storage = 0;

INSERT INTO t_ttl_quoted SELECT number, now('UTC') FROM numbers(1000);

-- A provable constant +100 day extension with no row expired before or after, so the fast path applies:
-- the ALTER must succeed and read no data at all. Reading every row would mean the fingerprint failed to
-- round-trip and the part fell back to the regular rewrite.
ALTER TABLE t_ttl_quoted MODIFY TTL `create time` + INTERVAL 400 DAY;
SELECT count() FROM t_ttl_quoted;

SYSTEM FLUSH LOGS part_log;
SELECT read_rows FROM system.part_log
WHERE database = currentDatabase() AND table = 't_ttl_quoted' AND event_type = 'MutatePart';

-- The shifted bounds must be the ones a full recomputation would produce: 400 days from the insert time.
SELECT dateDiff('day', now('UTC'), delete_ttl_info_max) FROM system.parts
WHERE database = currentDatabase() AND table = 't_ttl_quoted' AND active;

DROP TABLE t_ttl_quoted;
