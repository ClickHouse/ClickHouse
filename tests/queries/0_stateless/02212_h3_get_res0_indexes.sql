-- Tags: no-fasttest

select h3GetRes0Indexes();
select h3GetRes0Indexes(3); -- { serverError 42 }
