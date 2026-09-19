WITH
    proportionsZTest(0, 10, 100, 100, 0.95, 'unpooled') AS unpooled_x,
    proportionsZTest(10, 0, 100, 100, 0.95, 'unpooled') AS unpooled_y,
    proportionsZTest(0, 10, 100, 100, 0.95, 'pooled') AS pooled_x,
    proportionsZTest(10, 0, 100, 100, 0.95, 'pooled') AS pooled_y
SELECT
    isFinite(tupleElement(unpooled_x, 1))
    AND isFinite(tupleElement(unpooled_x, 2))
    AND isFinite(tupleElement(unpooled_x, 3))
    AND isFinite(tupleElement(unpooled_x, 4))
    AND isFinite(tupleElement(unpooled_y, 1))
    AND isFinite(tupleElement(unpooled_y, 2))
    AND isFinite(tupleElement(unpooled_y, 3))
    AND isFinite(tupleElement(unpooled_y, 4))
    AND isFinite(tupleElement(pooled_x, 1))
    AND isFinite(tupleElement(pooled_x, 2))
    AND isFinite(tupleElement(pooled_x, 3))
    AND isFinite(tupleElement(pooled_x, 4))
    AND isFinite(tupleElement(pooled_y, 1))
    AND isFinite(tupleElement(pooled_y, 2))
    AND isFinite(tupleElement(pooled_y, 3))
    AND isFinite(tupleElement(pooled_y, 4));
