-- An explicitly requested new meaning is persisted unambiguously as `toTimeWithoutDate`, so a later
-- reload under the legacy setting cannot flip the definition to the legacy semantics.

DROP TABLE IF EXISTS t_totime_explicit_new;

SET use_legacy_to_time = 0;

CREATE TABLE t_totime_explicit_new (c0 DateTime('UTC')) ENGINE = MergeTree ORDER BY toTime(c0);
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_explicit_new';
INSERT INTO t_totime_explicit_new VALUES ('2020-01-02 03:04:05');

-- The alias denotes the conversion regardless of the setting.
SELECT toTypeName(toTimeWithoutDate(c0)) FROM t_totime_explicit_new SETTINGS use_legacy_to_time = 0;
SELECT toTypeName(toTimeWithoutDate(c0)) FROM t_totime_explicit_new SETTINGS use_legacy_to_time = 1;

-- Reload under the opposite setting: the resolved key type must not move.
DETACH TABLE t_totime_explicit_new;
SET use_legacy_to_time = 1;
ATTACH TABLE t_totime_explicit_new;
SELECT count() FROM t_totime_explicit_new;
INSERT INTO t_totime_explicit_new VALUES ('2020-01-02 03:04:06');
SELECT count() FROM t_totime_explicit_new;

DROP TABLE t_totime_explicit_new;
