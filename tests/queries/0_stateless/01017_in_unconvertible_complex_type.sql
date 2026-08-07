-- When left and right element types are compatible, but the particular value
-- on the right is not in the range of the left type, it should be ignored.
select (toUInt8(1)) in (-1);
select (toUInt8(0)) in (-1);
select (toUInt8(255)) in (-1);

select [toUInt8(1)] in [-1];
select [toUInt8(0)] in [-1];
select [toUInt8(255)] in [-1];

-- When left and right element types are not compatible, we should get an error.
select (toUInt8(1)) in ('a'); -- { serverError TYPE_MISMATCH }
select [toUInt8(1)] in ['a']; -- { serverError TYPE_MISMATCH }
