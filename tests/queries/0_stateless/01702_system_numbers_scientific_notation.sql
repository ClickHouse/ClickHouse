select * from numbers(1e2) format Null;
select * from numbers_mt(1e2) format Null;
select * from numbers_mt('100') format Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select * from numbers_mt(inf) format Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
select * from numbers_mt(nan) format Null; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
