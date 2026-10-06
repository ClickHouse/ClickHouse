-- { echoOn }
-- ascii
select tokens('a_b a_3 a_ _a __a_b_3_', 'asciiCJK');
select tokens('3_b 3_3 3_ _3 __3_4_3_', 'asciiCJK');
select tokens('___ _3___a_ _a__a_b_c_ ___', 'asciiCJK');

select tokens('a:b a:3 a: :a ::a:b:3:', 'asciiCJK');
select tokens('3:b 3:3 3: :3 ::3:4:3:', 'asciiCJK');
select tokens('::: :3:::a: :a::a:b:c: :::', 'asciiCJK');

select tokens($$a'b a'3 a' 'a ''a'b'3'$$, 'asciiCJK');
select tokens($$3'b 3'3 3' '3 ''3'4'3'$$, 'asciiCJK');
select tokens($$''' '3'''a' 'a''a'b'c' '''$$, 'asciiCJK');

select tokens($$a.b a.3 a. .a ..a.b.3.$$, 'asciiCJK');
select tokens($$3.b 3.3 3. .3 ..3.4.3.$$, 'asciiCJK');
select tokens($$... .3...a. .a..a.b.c. ...$$, 'asciiCJK');

-- ascii and Chinese
select tokens('错误503', 'asciiCJK');
select tokens('taichi张三丰in the house', 'asciiCJK');

-- edge cases
select tokens('', 'asciiCJK');                               -- empty string

select tokensForLikePattern('%abc', 'asciiCJK');            -- wildcard before token
select tokensForLikePattern('abc%', 'asciiCJK');            -- token before wildcard
select tokensForLikePattern('a_b%c_d', 'asciiCJK');         -- wildcard in between
select tokensForLikePattern('a\%b a\_c', 'asciiCJK');       -- escaped % breaks token (not alnum), escaped _ joins token
select tokensForLikePattern('a\_b%c', 'asciiCJK');          -- mix escaped _ and unescaped %
select tokensForLikePattern('a\_b\%c', 'asciiCJK');         -- all escaped wildcards
select tokensForLikePattern('_a c%', 'asciiCJK');           -- leading and trailing wildcards

select tokensForLikePattern($$a:b%cd\_e.f'g$$, 'asciiCJK'); -- mix colon, dot, single quote, wildcard, escaped _
select tokensForLikePattern('%%__abc', 'asciiCJK');         -- multiple leading wildcards
select tokensForLikePattern('', 'asciiCJK');                -- empty string
select tokensForLikePattern('%%__', 'asciiCJK');            -- only wildcards
select tokensForLikePattern('你', 'asciiCJK');              -- single Unicode char
select tokensForLikePattern('abc你好', 'asciiCJK');         -- ASCII then Unicode
select tokensForLikePattern('你好%世界', 'asciiCJK');       -- Unicode token with wildcards

set enable_analyzer = 1;
set enable_full_text_index = 1;

drop table if exists tab;

create table tab (key UInt64, str String, index text_idx(str) type text(tokenizer = asciiCJK)) engine MergeTree order by key;

insert into tab values (1, 'hello错误502需要处理kitty');

explain estimate select * from tab where str like '%错误502需要%';

explain estimate select * from tab where str like '%错误503需要%';

drop table tab;

-- backward compatibility: unicodeWord alias
select tokens('taichi张三丰in the house', 'unicodeWord');

drop table if exists tab2;
create table tab2 (key UInt64, str String, index text_idx(str) type text(tokenizer = unicodeWord)) engine MergeTree order by key;
insert into tab2 values (1, 'hello错误502需要处理kitty');
explain estimate select * from tab2 where str like '%错误502需要%';
drop table tab2;
