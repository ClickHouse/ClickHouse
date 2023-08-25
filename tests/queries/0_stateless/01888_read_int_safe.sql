select toInt64('--1'); -- { serverError 72; }
select toInt64('+-1'); -- { serverError 72; }
select toInt64('++1'); -- { serverError 72; }
select toInt64('++'); -- { serverError 72; }
select toInt64('+'); -- { serverError 72; }
select toInt64('1+1'); -- { serverError 6; }
select toInt64('1-1'); -- { serverError 6; }
select toInt64(''); -- { serverError 32; }
select toInt64('1');
select toInt64('-1');
