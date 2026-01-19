SET enable_analyzer = 1;

DROP TABLE IF EXISTS segmented_ctr_cache;
DROP TABLE IF EXISTS bookmarks_join;
DROP TABLE IF EXISTS cart_join;

create table if not exists segmented_ctr_cache
(
    product_id        Int32,
    segment_id        Int32,
    count_in_viewport UInt64,
    count_in_viewed   UInt64
)
    engine = Memory;

INSERT INTO segmented_ctr_cache VALUES (1182604, 44, 15, 3);
INSERT INTO segmented_ctr_cache VALUES (311577, 52, 4, 2);
INSERT INTO segmented_ctr_cache VALUES (284246, 45, 2, 2);
INSERT INTO segmented_ctr_cache VALUES (1115559, 52, 9, 2);
INSERT INTO segmented_ctr_cache VALUES (1551941, 0, 163, 4);
INSERT INTO segmented_ctr_cache VALUES (7165089, 45, 17, 1);



CREATE TABLE bookmarks_join
(
  product_id Int32,
  segment_id Int32,
  count_in_bookmark Int32
) ENGINE = Join(ALL, LEFT, product_id, segment_id);
INSERT INTO bookmarks_join VALUES (1182604, 44, 1);
INSERT INTO bookmarks_join VALUES (311577, 52, 1);
INSERT INTO bookmarks_join VALUES (7165089, 0, 1);
INSERT INTO bookmarks_join VALUES (7165089, 50, 1);


CREATE TABLE cart_join
(
product_id Int32,
segment_id Int32,
count_in_cart Int32
) ENGINE = Join(ALL, LEFT, product_id, segment_id);

INSERT INTO cart_join VALUES (311577, 44, 1);
INSERT INTO cart_join VALUES (311577, 52, 1);
INSERT INTO cart_join VALUES (7165089, 0, 1);
INSERT INTO cart_join VALUES (7165089, 45, 3);


SELECT
    segmented_ctr_cache.product_id,
    segmented_ctr_cache.segment_id,
    count_in_bookmark,
    count_in_cart
FROM segmented_ctr_cache
LEFT JOIN cart_join ON
          cart_join.product_id = segmented_ctr_cache.product_id
      AND cart_join.segment_id = segmented_ctr_cache.segment_id
LEFT JOIN bookmarks_join ON
          bookmarks_join.product_id = segmented_ctr_cache.product_id
      AND bookmarks_join.segment_id = segmented_ctr_cache.segment_id
ORDER BY ALL;

DROP TABLE segmented_ctr_cache;
DROP TABLE bookmarks_join;
DROP TABLE cart_join;
