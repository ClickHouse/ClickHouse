select toDateTime(0, '/abc'); -- { serverError POCO_EXCEPTION }
select toDateTime(0, './abc'); -- { serverError POCO_EXCEPTION }
select toDateTime(0, '../abc'); -- { serverError POCO_EXCEPTION }
select toDateTime(0, '~/abc'); -- { serverError POCO_EXCEPTION }
select toDateTime(0, 'abc/../../cba'); -- { serverError POCO_EXCEPTION }

