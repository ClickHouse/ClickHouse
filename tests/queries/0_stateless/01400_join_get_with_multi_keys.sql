DROP TABLE IF EXISTS test_joinGet;

CREATE TABLE test_joinGet(a String, b String, c Float64) ENGINE = Join(any, left, a, b);

INSERT INTO test_joinGet VALUES ('ab', '1', 0.1), ('ab', '2', 0.2), ('cd', '3', 0.3);

SELECT joinGet(test_joinGet, 'c', 'ab', '1');

CREATE TABLE test_lc(a LowCardinality(String), b LowCardinality(String), c Float64) ENGINE = Join(any, left, a, b);

INSERT INTO test_lc VALUES ('ab', '1', 0.1), ('ab', '2', 0.2), ('cd', '3', 0.3);

SELECT joinGet(test_lc, 'c', 'ab', '1');

DROP TABLE test_joinGet;
DROP TABLE test_lc;
