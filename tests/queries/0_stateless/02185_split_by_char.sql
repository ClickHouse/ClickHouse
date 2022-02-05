select splitByChar(',', '1,2,3');
select splitByChar(',', '1,2,3', 0);
select splitByChar(',', '1,2,3', 1);
select splitByChar(',', '1,2,3', 2);
select splitByChar(',', '1,2,3', 3);

select splitByChar(',', '1,2,3', -2); -- { serverError 44 }
select splitByChar(',', '1,2,3', ''); -- { serverError 43 }