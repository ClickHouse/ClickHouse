DROP DATABASE IF EXISTS test_04812;

CREATE DATABASE test_04812;
CREATE TABLE test_04812.data (id UInt64) ENGINE = Memory;
INSERT INTO test_04812.data VALUES (1), (2);
CREATE VIEW test_04812.filtered AS
SELECT * FROM test_04812.data WHERE id = {id:UInt64};

SET param_view = 'test_04812.filtered';

-- A qualified value for an `Identifier` query parameter must be treated as a compound function name
-- by both legacy execution and the separate `EXPLAIN SYNTAX` expansion path.
SELECT id FROM {view:Identifier}(id = 2) SETTINGS enable_analyzer = 0;
SELECT count()
FROM (EXPLAIN SYNTAX SELECT * FROM {view:Identifier}(id = 1))
WHERE explain LIKE '%WHERE id = 1%';

DROP DATABASE test_04812;
