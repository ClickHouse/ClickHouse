DROP TABLE IF EXISTS test.pk_set;

CREATE TABLE test.pk_set (d Date, n UInt64, host String, code UInt64) ENGINE = MergeTree(d, (n, host, code), 1);
INSERT INTO test.pk_set (n, host, code) VALUES (1, 'market', 100), (11, 'news', 100);

SELECT count() FROM test.pk_set WHERE host IN ('admin.market1', 'admin.market2') AND code = 100;
SELECT count() FROM test.pk_set WHERE host IN ('admin.market1', 'admin.market2') AND code = 100 AND n = 11;
SELECT count() FROM test.pk_set WHERE host IN ('admin.market1', 'admin.market2') AND code = 100 AND n >= 11;
SELECT count() FROM test.pk_set WHERE host IN ('market', 'admin.market2', 'admin.market3', 'admin.market4', 'abc') AND code = 100 AND n = 11;
SELECT count() FROM test.pk_set WHERE host IN ('market', 'admin.market2', 'admin.market3', 'admin.market4', 'abc') AND code = 100 AND n >= 11;
SELECT count() FROM test.pk_set WHERE host IN ('admin.market2', 'admin.market3', 'admin.market4', 'abc') AND code = 100 AND n = 11;
SELECT count() FROM test.pk_set WHERE host IN ('admin.market2', 'admin.market3', 'admin.market4', 'abc', 'news') AND code = 100 AND n = 11;

DROP TABLE test.pk_set;
