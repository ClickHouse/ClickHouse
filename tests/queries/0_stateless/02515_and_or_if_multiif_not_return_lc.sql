select toTypeName(if(toLowCardinality(number % 2), 1, 2)) from numbers(1);
select toTypeName(multiIf(toLowCardinality(number % 2), 1, 1, 2, 3)) from numbers(1);
select toTypeName(toLowCardinality(number % 2) and 2) from numbers(1);
select toTypeName(toLowCardinality(number % 2) or 2) from numbers(1);

