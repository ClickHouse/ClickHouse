DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults (a UInt8, b DEFAULT 0, c DEFAULT identity(b)) ENGINE = Memory;
INSERT INTO defaults (a) VALUES (1);
SELECT * FROM defaults;
DROP TABLE defaults;

DROP TABLE IF EXISTS elog_cut;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE elog_cut
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

INSERT INTO elog_cut (uts, pr, ya_uid, adf_uid, owner_id) VALUES ('2015-01-01 01:02:03', 111, 123, 456, 789);
SELECT date, uts, pr, ya_uid, adf_uid, owner_id, eff_uid, page_session, sample_key FROM elog_cut;
DROP TABLE elog_cut;
