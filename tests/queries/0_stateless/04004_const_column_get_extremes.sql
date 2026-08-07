-- Regression test for ColumnConst::getExtremes passing outer start/end
-- to the inner column, causing an out-of-bounds access in the null map.
-- https://github.com/ClickHouse/ClickHouse/issues/98240

SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;
SET allow_suspicious_variant_types = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET extremes = 1;

CREATE TABLE IF NOT EXISTS 04004_lc (x LowCardinality(String)) ENGINE = Memory;
CREATE TABLE IF NOT EXISTS 04004_other (x DateTime) ENGINE = Memory;
INSERT INTO 04004_lc VALUES ('a'), ('b'), ('c');

SELECT equals(CAST(NULL AS Nullable(UInt8)), x) FROM merge(currentDatabase(), '^04004_') SETTINGS optimize_move_to_prewhere = 0;

DROP TABLE 04004_lc;
DROP TABLE 04004_other;
