DROP TABLE IF EXISTS test_rlp;

CREATE TABLE test_rlp (a Int32, b Int32) ENGINE=MergeTree() ORDER BY a SETTINGS index_granularity=5, index_granularity_bytes = '10Mi';

INSERT INTO test_rlp SELECT number, number FROM numbers(15);

ALTER TABLE test_rlp ADD COLUMN c Int32 DEFAULT b+10;

-- { echoOn }

SELECT a, c FROM test_rlp WHERE c%2 == 0 AND b < 5;

DROP POLICY IF EXISTS test_rlp_policy ON test_rlp;

CREATE ROW POLICY test_rlp_policy ON test_rlp FOR SELECT USING c%2 == 0 TO default;

SELECT a, c FROM test_rlp WHERE b < 5 SETTINGS optimize_move_to_prewhere = 0;

SELECT a, c FROM test_rlp PREWHERE b < 5;

-- { echoOff }

DROP POLICY test_rlp_policy ON test_rlp;

DROP TABLE test_rlp;
