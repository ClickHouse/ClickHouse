-- Tags: no-replicated-database
-- Tag no-replicated-database: a `Replicated` database only allows `DETACH TABLE ... PERMANENTLY`.

-- Creating an `ABR` table requires `allow_experimental_abr`, while loading and querying it do not.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS abr;
DROP TABLE IF EXISTS src_1;

CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 VALUES ('2026-01-10 00:00:00', 1);

SET allow_experimental_abr = 0;
CREATE TABLE abr ENGINE = ABR('src', [1], ('ts')); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_abr = 1;
CREATE TABLE abr ENGINE = ABR('src', [1], ('ts'));

SET allow_experimental_abr = 0;
DETACH TABLE abr;
ATTACH TABLE abr;
SELECT count() FROM abr WHERE ts >= toDateTime('2026-01-01 00:00:00');

DROP TABLE abr;
DROP TABLE src_1;
