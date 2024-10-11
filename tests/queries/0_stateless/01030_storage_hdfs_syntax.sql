-- Tags: no-fasttest, use-hdfs

drop table if exists test_table_hdfs_syntax
;
create table test_table_hdfs_syntax (id UInt32) ENGINE = HDFS('')
; -- { serverError BAD_ARGUMENTS }
create table test_table_hdfs_syntax (id UInt32) ENGINE = HDFS('','','', '')
; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
drop table if exists test_table_hdfs_syntax
;
