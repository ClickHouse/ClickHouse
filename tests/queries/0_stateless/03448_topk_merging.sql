DROP TABLE IF EXISTS topk_test;

CREATE TABLE topk_test (
    foo UInt64,
    top_items AggregateFunction(topKWeighted(100, 3, 'counts'), String, UInt64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (foo);

INSERT INTO topk_test
SELECT
    6 AS foo,
    topKWeightedState(100, 3, 'counts')(item, toUInt64(10))
FROM (
    SELECT arrayJoin(['a','a','a','b','b','b','c','c','c','d','d','e','e']) AS item
);

INSERT INTO topk_test
SELECT
    8 AS foo,
    topKWeightedState(100, 3, 'counts')(item, toUInt64(10))
FROM (
    SELECT arrayJoin(['i','i','i','j','j','j','k','k','k','d','d','e','e']) AS item
);

INSERT INTO topk_test
SELECT
    9 AS foo,
    topKWeightedState(100, 3, 'counts')(item, toUInt64(10))
FROM (
    SELECT arrayJoin(['l','l','l','m','m','m','n','n','n','d','d','e','e']) AS item
);

INSERT INTO topk_test
SELECT
    10 AS foo,
    topKWeightedState(100, 3, 'counts')(item, toUInt64(10))
FROM (
    SELECT arrayJoin(['z','z','z','w','w','w','y','y','y','d','d','e','e']) AS item
);

INSERT INTO topk_test
SELECT
    11 AS foo,
    topKWeightedState(100, 3, 'counts')(item, toUInt64(10))
FROM (
    SELECT arrayJoin(['i','i','i','j','j','j','k','k','k','d','d','e','e']) AS item
);

SELECT * FROM (
    SELECT
        foo,
        untuple(arrayJoin(topKWeightedMerge(100, 3, 'counts')(top_items))) AS top
    FROM topk_test
    GROUP BY foo
)
ORDER BY foo, top.count DESC, top.item;

SELECT * FROM (
    SELECT
        untuple(arrayJoin(topKWeightedMerge(100, 3, 'counts')(top_items))) AS top
    FROM topk_test FINAL
)
ORDER BY top.count DESC, top.item;

DROP TABLE topk_test;
