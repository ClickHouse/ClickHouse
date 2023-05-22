create table some_table(str String) engine=MergeTree primary key(str);
create table some_table_utf8(str String) engine=MergeTree primary key(str);

insert into some_table(str) values ('good good bad'), ('bad bad good');
insert into some_table_utf8(str) values ('добро добро зло'), ('зло муки добро');

select str, ngramClassify('offensive', str) from some_table;
select str, ngramClassifyUTF8('offensive-utf8', str) from some_table_utf8;