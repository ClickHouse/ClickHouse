create table test_columns(id UInt64, _part UInt8) engine=MergeTree order by id; -- { serverError ILLEGAL_COLUMN }
