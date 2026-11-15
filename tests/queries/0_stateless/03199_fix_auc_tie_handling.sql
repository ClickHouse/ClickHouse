CREATE TABLE labels_unordered
(
    idx Int64,
    score Float64,
    label Int64
)
ENGINE = MergeTree
PRIMARY KEY idx
ORDER BY idx;

SELECT floor(arrayAUC(array_concat_agg([score]), array_concat_agg([label])), 5)
FROM labels_unordered;

INSERT INTO labels_unordered (idx,score,label) VALUES (1,0.1,0), (2,0.35,1), (3,0.4,0), (4,0.8,1), (5,0.8,0);

SELECT floor(arrayAUC(array_concat_agg([score]), array_concat_agg([label])), 5)
FROM labels_unordered;

CREATE TABLE labels_ordered
(
    idx Int64,
    score Float64,
    label Int64
)
ENGINE = MergeTree
PRIMARY KEY idx
ORDER BY idx;

INSERT INTO labels_ordered (idx,score,label) VALUES (1,0.1,0), (2,0.35,1), (3,0.4,0), (4,0.8,0), (5,0.8,1);

SELECT floor(arrayAUC(array_concat_agg([score]), array_concat_agg([label])), 5)
FROM labels_ordered;