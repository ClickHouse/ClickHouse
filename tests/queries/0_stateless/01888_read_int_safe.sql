select toInt64('--1'); -- { serverError CANNOT_PARSE_NUMBER }
select toInt64('+-1'); -- { serverError CANNOT_PARSE_NUMBER }
select toInt64('++1'); -- { serverError CANNOT_PARSE_NUMBER }
select toInt64('++'); -- { serverError CANNOT_PARSE_NUMBER }
select toInt64('+'); -- { serverError CANNOT_PARSE_NUMBER }
select toInt64('1+1'); -- { serverError CANNOT_PARSE_TEXT }
select toInt64('1-1'); -- { serverError CANNOT_PARSE_TEXT }
select toInt64(''); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
select toInt64('1');
select toInt64('-1');
