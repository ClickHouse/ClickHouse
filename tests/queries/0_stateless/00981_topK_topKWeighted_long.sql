DROP TABLE IF EXISTS topk;

CREATE TABLE topk (val1 String, val2 UInt32) ENGINE = MergeTree ORDER BY val1;

INSERT INTO topk WITH number % 7 = 0 AS frequent SELECT toString(frequent ? number % 10 : number), frequent ? 999999999 : number FROM numbers(4000000);

SELECT arraySort(topK(10)(val1)) FROM topk;
SELECT arraySort(topKWeighted(10)(val1, val2)) FROM topk;
SELECT topKWeighted(10)(toString(number), number) from numbers(3000000);

DROP TABLE topk;
