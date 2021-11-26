-- Tags: no-fasttest
-- Tag no-fasttest: needs s2

select s2CapUnion(3814912406305146967, 1.0, 1157347770437378819, 1.0);
select s2CapUnion(1157339245694594829, -1.0, 1152921504606846977, -1.0);
select s2CapUnion(1157339245694594829, toFloat64(toUInt64(-1)), 1157339245694594829, toFloat64(toUInt64(-1)));


select s2CapUnion(nan, 3.14, 1157339245694594829, 3.14); -- { serverError 43 }
select s2CapUnion(1157339245694594829, nan, 1157339245694594829, 3.14); -- { serverError 43 }
select s2CapUnion(1157339245694594829, 3.14, nan, 3.14); -- { serverError 43 }
select s2CapUnion(1157339245694594829, 3.14, 1157339245694594829, nan); -- { serverError 43 }
