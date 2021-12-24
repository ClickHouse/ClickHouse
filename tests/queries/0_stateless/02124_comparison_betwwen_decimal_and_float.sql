select CAST(1.0, 'Decimal(15,2)') > CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') = CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') < CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') != CAST(1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') > CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') = CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') < CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') != CAST(-1, 'Float64');
select CAST(1.0, 'Decimal(15,2)') > CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') = CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') < CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') != CAST(1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') > CAST(-1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') = CAST(-1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') < CAST(-1, 'Float32');
select CAST(1.0, 'Decimal(15,2)') != CAST(-1, 'Float32');

SELECT toDecimal32('11.00', 2) > 1.;

SELECT 0.1000000000000000055511151231257827021181583404541015625::Decimal256(70) = 0.1;
