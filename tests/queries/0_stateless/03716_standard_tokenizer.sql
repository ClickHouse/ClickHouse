-- ascii
select tokens('a_b a_3 a_ _a __a_b_3_', 'standard');
select tokens('3_b 3_3 3_ _3 __3_4_3_', 'standard');
select tokens('___ _3___a_ _a__a_b_c_ ___', 'standard');

select tokens('a:b a:3 a: :a ::a:b:3:', 'standard');
select tokens('3:b 3:3 3: :3 ::3:4:3:', 'standard');
select tokens('::: :3:::a: :a::a:b:c: :::', 'standard');

select tokens($$a'b a'3 a' 'a ''a'b'3'$$, 'standard');
select tokens($$3'b 3'3 3' '3 ''3'4'3'$$, 'standard');
select tokens($$''' '3'''a' 'a''a'b'c' '''$$, 'standard');

select tokens($$a.b a.3 a. .a ..a.b.3.$$, 'standard');
select tokens($$3.b 3.3 3. .3 ..3.4.3.$$, 'standard');
select tokens($$... .3...a. .a..a.b.c. ...$$, 'standard');

-- ascii and Chinese
select tokens('错误503', 'standard');
select tokens('taichi张三丰in the house', 'standard');

-- stop words
select tokens('错误，503', 'standard'); -- default stop words contains common full-width separators
select tokens('错误and 503', 'standard', ['and']);


set allow_experimental_full_text_index = 1;

drop table if exists tab;

create table tab (key UInt64, str String, index text_idx(str) type text(tokenizer = standard)) engine MergeTree order by key;

insert into tab values (1, 'hello错误502需要处理kitty');

explain estimate select * from tab where str like '%错误502需要%';

explain estimate select * from tab where str like '%错误503需要%';

drop table tab;
