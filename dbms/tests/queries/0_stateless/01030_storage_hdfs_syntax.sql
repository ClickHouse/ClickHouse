drop table if exists test_table_hdfs_syntax
;
create table test_table_hdfs_syntax (id UInt32) ENGINE = HDFS('')
; -- { serverError 42 }
create table test_table_hdfs_syntax (id UInt32) ENGINE = HDFS('','','', '')
; -- { serverError 42 }
drop table if exists test_table_hdfs_syntax
;
