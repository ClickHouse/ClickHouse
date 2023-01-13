drop table if exists test_table_url_syntax
;
create table test_table_url_syntax (id UInt32) ENGINE = URL('')
; -- { serverError 36 }
create table test_table_url_syntax (id UInt32) ENGINE = URL('','','','')
; -- { serverError 42 }
drop table if exists test_table_url_syntax
;
