-- `JSONL` is an input/output alias of `JSONEachRow` (like `JSONLines` and `NDJSON`).
-- It must work for both output (SELECT) and input (INSERT), not just input.

SELECT number::UInt32 AS n FROM numbers(3) FORMAT JSONL;

DROP TABLE IF EXISTS t_jsonl;
CREATE TABLE t_jsonl (n1 UInt32, n2 UInt32) ENGINE = Memory;

INSERT INTO t_jsonl FORMAT JSONL {"n1": 1, "n2": 2} {"n1": 3, "n2": 4} {"n1": 5, "n2": 6};

SELECT * FROM t_jsonl ORDER BY n1, n2 FORMAT JSONL;

DROP TABLE t_jsonl;
