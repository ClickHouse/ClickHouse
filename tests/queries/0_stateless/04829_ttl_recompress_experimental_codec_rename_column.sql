-- `RENAME COLUMN` rewrites the stored TTL AST (renaming the columns it references) without touching
-- the recompression codec, so a table whose `TTL ... RECOMPRESS` uses an experimental codec (created
-- under `allow_experimental_codecs = 1`) must allow renaming a column the TTL references in sessions
-- without the opt-in: the exemption from the experimental-codec gate is keyed off the codec itself,
-- not off the whole TTL AST staying byte-identical.

DROP TABLE IF EXISTS t_ttl_zxc_rename;

SET allow_experimental_codecs = 1;

CREATE TABLE t_ttl_zxc_rename (d Date, x UInt64)
ENGINE = MergeTree ORDER BY tuple()
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZXC);

SET allow_experimental_codecs = 0;

-- Renaming the column the TTL references rewrites the TTL AST but keeps the codec: not gated.
ALTER TABLE t_ttl_zxc_rename RENAME COLUMN d TO event_date;
SELECT 'rename referenced column ok';

-- The rename is durable and the TTL still carries the codec.
SELECT extract(create_table_query, 'TTL [^S]+ RECOMPRESS CODEC\(ZXC\)')
FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_zxc_rename';

-- Renaming an unreferenced column is unrelated to the TTL: not gated either.
ALTER TABLE t_ttl_zxc_rename RENAME COLUMN x TO value;
SELECT 'rename unreferenced column ok';

-- Changing the TTL itself is fresh DDL and stays gated.
ALTER TABLE t_ttl_zxc_rename MODIFY TTL event_date + INTERVAL 2 MONTH RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }
SELECT 'modify ttl gated';

DROP TABLE t_ttl_zxc_rename;
