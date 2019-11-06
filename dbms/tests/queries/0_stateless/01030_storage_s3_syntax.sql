drop table if exists test_table_s3_syntax
;
create table test_table_s3_syntax (id UInt32) ENGINE = S3('')
; -- { serverError 42 }
create table test_table_s3_syntax (id UInt32) ENGINE = S3('','','')
; -- { serverError 42 }
drop table if exists test_table_s3_syntax
;
