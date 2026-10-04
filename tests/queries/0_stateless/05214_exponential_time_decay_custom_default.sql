SET allow_experimental_json_type = 1;
SET allow_experimental_time_decay_aggregate_functions = 1;

DROP TABLE IF EXISTS test_exponential_time_decay_json_default;
CREATE TABLE test_exponential_time_decay_json_default
(
    json JSON(value ExponentialTimeDecaying(10))
)
ENGINE = Memory;

-- A whole-object NULL takes the JSON typed-path default insertion path. The
-- semantic default must keep the decay-length marker from the custom type.
INSERT INTO test_exponential_time_decay_json_default VALUES ('null');

SELECT exponentialTimeDecayingValueAt(json.value, toFloat64(0))
FROM test_exponential_time_decay_json_default
FORMAT Null;

DROP TABLE test_exponential_time_decay_json_default;
