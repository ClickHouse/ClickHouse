DROP TABLE IF EXISTS tags;

CREATE TABLE tags (
    id String,
    seqs Array(UInt8),
    create_time DateTime DEFAULT now()
) engine=ReplacingMergeTree()
ORDER BY (id);

INSERT INTO tags(id, seqs) VALUES ('id1', [1,2,3]), ('id2', [0,2,3]), ('id1', [1,3]);

WITH
    (SELECT [0, 1, 2, 3]) AS arr1
SELECT arrayIntersect(argMax(seqs, create_time), arr1) AS common, id
FROM tags
WHERE id LIKE 'id%'
GROUP BY id;

DROP TABLE tags;
