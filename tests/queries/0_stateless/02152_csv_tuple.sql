drop table if exists test_02152;
create table test_02152 (x UInt32, y String, z Array(UInt32), t Tuple(UInt32, String, Array(UInt32))) engine=File('CSV') settings format_csv_delimiter=';';
insert into test_02152 select 1, 'Hello', [1,2,3], tuple(2, 'World', [4,5,6]); 
select * from test_02152;
drop table test_02152;

create table test_02152 (x UInt32, y String, z Array(UInt32), t Tuple(UInt32, String, Array(UInt32))) engine=File('CustomSeparated') settings format_custom_field_delimiter='<field_delimiter>', format_custom_row_before_delimiter='<row_start>', format_custom_row_after_delimiter='<row_end_delimiter>', format_custom_escaping_rule='CSV';
insert into test_02152 select 1, 'Hello', [1,2,3], tuple(2, 'World', [4,5,6]);
select * from test_02152;
drop table test_02152;

