DROP TABLE IF EXISTS topk;

CREATE TABLE topk (val1 String, val2 UInt32) ENGINE = MergeTree ORDER BY val1;

INSERT INTO topk SELECT toString(number), number FROM numbers(3000000);
INSERT INTO topk SELECT toString(number % 10), 999999999 FROM numbers(1000000);

SELECT arraySort(topK(10)(val1)) FROM topk;
SELECT arraySort(topKWeighted(10)(val1, val2)) FROM topk;
SELECT topKWeighted(10)(toString(number), number) from numbers(3000000);

DROP TABLE topk;
