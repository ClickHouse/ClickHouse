select toDateTime(0, '/abc'); -- { serverError BAD_ARGUMENTS }
select toDateTime(0, './abc'); -- { serverError BAD_ARGUMENTS }
select toDateTime(0, '../abc'); -- { serverError BAD_ARGUMENTS }
select toDateTime(0, '~/abc'); -- { serverError BAD_ARGUMENTS }
select toDateTime(0, 'abc/../../cba'); -- { serverError BAD_ARGUMENTS }

