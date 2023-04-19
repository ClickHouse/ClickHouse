select splitByChar(',', '1,2,3');
select splitByChar(',', '1,2,3', 0);
select splitByChar(',', '1,2,3', 1);
select splitByChar(',', '1,2,3', 2);
select splitByChar(',', '1,2,3', 3);

select splitByChar(',', '1,2,3', -2); -- { serverError 44 }
select splitByChar(',', '1,2,3', ''); -- { serverError 43 }

SELECT splitByChar('=', s, 1) FROM values('s String', 'expr1=1+1=2', 'expr2=2+2=4=1+3')
