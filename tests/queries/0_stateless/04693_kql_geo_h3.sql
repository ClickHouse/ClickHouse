-- Tags: no-fasttest
-- The H3 cell functions map onto ClickHouse's `geoToH3` and friends, which come from the H3
-- library and are not built in the fast test.
--
-- Every expected value below is the one printed in Microsoft's own reference page for that
-- function.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- H3 cells are token strings --';
print geo_point_to_h3cell(-74.04450446039874, 40.689250859314974, 6);   // 862a1072fffffff
print geo_h3cell_level('862a1072fffffff');                 // 6
print geo_h3cell_parent('862a1072fffffff');                // 852a1073fffffff
print geo_h3cell_parent('862a1072fffffff', 1);             // 812a3ffffffffff
print geo_h3cell_level(geo_point_to_h3cell(1, 1, 10));     // 10
print array_length(geo_h3cell_children('862a1072fffffff'));   // 7
print array_length(geo_h3cell_neighbors('862a1072fffffff')); // 6

SET dialect = 'clickhouse';
