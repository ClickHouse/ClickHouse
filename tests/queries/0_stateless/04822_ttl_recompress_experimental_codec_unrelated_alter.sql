-- A table whose `TTL ... RECOMPRESS` uses an experimental codec (created under
-- `allow_experimental_codecs = 1`) must stay alterable in sessions without the opt-in:
-- an unrelated `ALTER` rebuilds the unchanged TTL from stored metadata, which is not fresh DDL.
-- Touching the TTL itself remains gated.

DROP TABLE IF EXISTS t_ttl_zxc_unrelated_alter;

SET allow_experimental_codecs = 1;

CREATE TABLE t_ttl_zxc_unrelated_alter (d Date, x UInt64)
ENGINE = MergeTree ORDER BY x
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZXC);

SET allow_experimental_codecs = 0;

-- Unrelated metadata rewrites must not re-enter the experimental-codec gate for the unchanged TTL.
ALTER TABLE t_ttl_zxc_unrelated_alter ADD COLUMN y UInt8;
ALTER TABLE t_ttl_zxc_unrelated_alter MODIFY COLUMN y UInt16;
ALTER TABLE t_ttl_zxc_unrelated_alter DROP COLUMN y;
SELECT 'unrelated alters ok';

-- Modifying the TTL is fresh DDL and stays gated.
ALTER TABLE t_ttl_zxc_unrelated_alter MODIFY TTL d + INTERVAL 2 MONTH RECOMPRESS CODEC(ZXC); -- { serverError BAD_ARGUMENTS }

SET allow_experimental_codecs = 1;
ALTER TABLE t_ttl_zxc_unrelated_alter MODIFY TTL d + INTERVAL 2 MONTH RECOMPRESS CODEC(ZXC);
SELECT 'modify ttl gated';

DROP TABLE t_ttl_zxc_unrelated_alter;
