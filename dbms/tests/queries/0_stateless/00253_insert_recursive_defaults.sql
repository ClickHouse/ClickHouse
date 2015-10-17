DROP TABLE IF EXISTS test.defaults;
CREATE TABLE test.defaults (a UInt8, b DEFAULT 0, c DEFAULT identity(b)) ENGINE = Memory;
INSERT INTO test.defaults (a) VALUES (1);
SELECT * FROM test.defaults;
DROP TABLE test.defaults;

DROP TABLE IF EXISTS test.elog_cut;
CREATE TABLE test.elog_cut
(
    date Date DEFAULT toDate(uts),
    uts DateTime,
    pr UInt64,
    ya_uid UInt64,
    adf_uid UInt64,
    owner_id UInt32,
    eff_uid UInt64 DEFAULT if(adf_uid != 0, adf_uid, ya_uid),
    page_session UInt64 DEFAULT cityHash64(eff_uid, pr),
    sample_key UInt64 ALIAS page_session
) ENGINE = MergeTree(date, cityHash64(adf_uid, ya_uid, pr), (owner_id, date, cityHash64(adf_uid, ya_uid, pr)), 8192);

INSERT INTO test.elog_cut (uts, pr, ya_uid, adf_uid, owner_id) VALUES ('2015-01-01 01:02:03', 111, 123, 456, 789);
SELECT date, uts, pr, ya_uid, adf_uid, owner_id, eff_uid, page_session, sample_key FROM test.elog_cut;
DROP TABLE test.elog_cut;
