-- Disable force_primary_key_reverse_order: Tests distinct-in-order optimization sensitive to sort direction
SET force_primary_key_reverse_order = 0;

set optimize_distinct_in_order=1;

DROP TABLE IF EXISTS test_string;

CREATE TABLE test_string
(
    `c1` String,
    `c2` String
)
ENGINE = MergeTree
ORDER BY c1;

INSERT INTO test_string(c1, c2) VALUES ('1',  ''), ('2', '');

SELECT DISTINCT c2, c1 FROM test_string;
