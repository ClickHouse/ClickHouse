-- Mutation expressions are persisted in mutation entries and resolved by the background executor
-- with the server default settings, so a legacy `toTime` spelling must be canonicalized in them:
-- the same expression cannot give one value in SELECT and another in UPDATE within one session.

DROP TABLE IF EXISTS t_totime_mutation;

SET use_legacy_to_time = 1;

CREATE TABLE t_totime_mutation (c0 DateTime('UTC'), v UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_totime_mutation VALUES ('2020-01-02 03:04:05', 0), ('2020-01-03 01:00:00', 0);

SELECT 'session', toUInt32(toTime(c0)) FROM t_totime_mutation ORDER BY c0;

ALTER TABLE t_totime_mutation UPDATE v = toUInt32(toTime(c0)) WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'updated', v FROM t_totime_mutation ORDER BY c0;

ALTER TABLE t_totime_mutation DELETE WHERE toUInt32(toTime(c0)) = 97445 SETTINGS mutations_sync = 2;
SELECT 'after_delete', toUInt32(toTime(c0)), v FROM t_totime_mutation;

DROP TABLE t_totime_mutation;
