DROP TABLE IF EXISTS t_final_collapsing;

CREATE TABLE t_final_collapsing
(
  key Int8,
  sign Int8
)
ENGINE = CollapsingMergeTree(sign) ORDER BY key;

INSERT INTO t_final_collapsing VALUES (5, -1);

OPTIMIZE TABLE t_final_collapsing FINAL; -- to move part to a level 1, to enable optimizations

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_final_collapsing FINAL;

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_final_collapsing FINAL;

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_final_collapsing FINAL;

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_final_collapsing FINAL;

DROP TABLE t_final_collapsing;

CREATE TABLE t_final_collapsing
(
  key Int8,
  sign Int8,
  version UInt64
)
ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY key;

INSERT INTO t_final_collapsing VALUES (5, -1, 1);

OPTIMIZE TABLE t_final_collapsing FINAL; -- to move part to a level 1, to enable optimizations

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_final_collapsing FINAL;

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 0, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_final_collapsing FINAL;

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 0;
SELECT count() FROM t_final_collapsing FINAL;

SET split_parts_ranges_into_intersecting_and_non_intersecting_final = 1, split_intersecting_parts_ranges_into_layers_final = 1;
SELECT count() FROM t_final_collapsing FINAL;

DROP TABLE t_final_collapsing;
