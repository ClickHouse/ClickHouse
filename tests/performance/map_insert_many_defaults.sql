SELECT
    throwIf(count() != {rows}, 'Unexpected Map benchmark row count'),
    sum(id),
    sum(length(m1)),
    sum(length(m2)),
    sum(length(m3)),
    sum(length(m4))
FROM format(
    JSONColumns,
    'id UInt64, m1 {map_type}, m2 {map_type}, m3 {map_type}, m4 {map_type}',
    concat('{{"id":[', repeat('0,', {rows} - 1), '0]}}'));
