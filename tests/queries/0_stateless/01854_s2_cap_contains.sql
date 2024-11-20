-- Tags: no-fasttest
-- Tag no-fasttest: needs s2

select s2CapContains(1157339245694594829, 1.0, 1157347770437378819);
select s2CapContains(1157339245694594829, 1.0, 1152921504606846977);
select s2CapContains(1157339245694594829, 3.14, 1157339245694594829);

select s2CapContains(nan, 3.14, 1157339245694594829); -- { serverError 43 }
select s2CapContains(1157339245694594829, nan, 1157339245694594829); -- { serverError 43 }
select s2CapContains(1157339245694594829, 3.14, nan); -- { serverError 43 }


select s2CapContains(toUInt64(-1), -1.0, toUInt64(-1)); -- { serverError 36 }
select s2CapContains(toUInt64(-1), 9999.9999, toUInt64(-1)); -- { serverError 36 }
