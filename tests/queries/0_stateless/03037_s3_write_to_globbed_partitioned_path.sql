-- Tags: no-fasttest

insert into function s3('http://localhost:11111/test/data_*_{_partition_id}.csv') partition by number % 3 select * from numbers(10); -- {serverError DATABASE_ACCESS_DENIED}

