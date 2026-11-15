set enable_analyzer=1;
SELECT concat('With ', materialize(_CAST('ba\0', 'LowCardinality(FixedString(3))'))) AS `concat('With ', materialize(CAST('ba\\0', 'LowCardinality(FixedString(3))')))` FROM system.one GROUP BY 'With ';
