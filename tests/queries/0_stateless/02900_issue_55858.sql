set precise_float_parsing = 1;

select cast('2023-01-01' as Float64); -- { serverError 6 }
select cast('2023-01-01' as Float32); -- { serverError 6 }
select toFloat32('2023-01-01'); -- { serverError 6 }
select toFloat64('2023-01-01'); -- { serverError 6 }
select toFloat32OrZero('2023-01-01');
select toFloat64OrZero('2023-01-01');
select toFloat32OrNull('2023-01-01');
select toFloat64OrNull('2023-01-01');
