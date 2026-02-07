-- Test iif() function in KQL dialect
DROP TABLE IF EXISTS iif_test;
CREATE TABLE iif_test
(    
    value Int32,
    name String
) ENGINE = Memory;
INSERT INTO iif_test VALUES (10, 'ten'), (20, 'twenty'), (30, 'thirty'), (5, 'five');

set allow_experimental_kusto_dialect=1;
set dialect = 'kusto';

print '-- iif() basic tests';
print iif(true, 'yes', 'no');
print iif(false, 'yes', 'no');
print iif(1 > 2, 'greater', 'less or equal');
print iif(10 < 20, 100, 200);

print '-- iif() with numeric types';
print iif(5 > 3, 1.5, 2.5);
print iif(5 < 3, 100, 200);

print '-- iif() with null values';
print iif(isnull(null), 'is null', 'not null');
print iif(isnotnull(5), 'has value', 'no value');

print '-- iif() nested';
print iif(true, iif(true, 'inner true', 'inner false'), 'outer false');
print iif(false, 'outer true', iif(true, 'inner true', 'inner false'));

print '-- iif() with table data';
iif_test | project name, category = iif(value > 15, 'high', 'low');
iif_test | project name, doubled = iif(value < 20, value * 2, value);
iif_test | where iif(value > 10, true, false) | project name, value;

set dialect = 'clickhouse';
DROP TABLE IF EXISTS iif_test;
