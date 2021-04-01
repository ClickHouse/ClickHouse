select 1 as x, x = 1 or x = 2 or x = 3 or x = -1;
select 1 as x, x = 1.0 or x = 2 or x = 3 or x = -1;
select 1 as x, x = 1.5 or x = 2 or x = 3 or x = -1;

SELECT
    1 IN (1, -1, 2.0, 2.5), 
    1.0 IN (1, -1, 2.0, 2.5), 
    1 IN (1.0, -1, 2.0, 2.5),
    1.0 IN (1.0, -1, 2.0, 2.5),
    1 IN (1.1, -1, 2.0, 2.5),
    -1 IN (1, -1, 2.0, 2.5);

SELECT -number IN (1, 2, 3, -5.0, -2.0) FROM system.numbers LIMIT 10;
