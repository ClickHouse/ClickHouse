-- { echoOn }
-- ascii
select tokens('a_b a_3 a_ _a __a_b_3_', 'unicodeWord');
select tokens('3_b 3_3 3_ _3 __3_4_3_', 'unicodeWord');
select tokens('___ _3___a_ _a__a_b_c_ ___', 'unicodeWord');

select tokens('a:b a:3 a: :a ::a:b:3:', 'unicodeWord');
select tokens('3:b 3:3 3: :3 ::3:4:3:', 'unicodeWord');
select tokens('::: :3:::a: :a::a:b:c: :::', 'unicodeWord');

select tokens($$a'b a'3 a' 'a ''a'b'3'$$, 'unicodeWord');
select tokens($$3'b 3'3 3' '3 ''3'4'3'$$, 'unicodeWord');
select tokens($$''' '3'''a' 'a''a'b'c' '''$$, 'unicodeWord');

select tokens($$a.b a.3 a. .a ..a.b.3.$$, 'unicodeWord');
select tokens($$3.b 3.3 3. .3 ..3.4.3.$$, 'unicodeWord');
select tokens($$... .3...a. .a..a.b.c. ...$$, 'unicodeWord');

-- ascii and Chinese
select tokens('错误503', 'unicodeWord');
select tokens('taichi张三丰in the house', 'unicodeWord');

-- edge cases
select tokens('', 'unicodeWord');                               -- empty string

select tokensForLikePattern('%abc', 'unicodeWord');            -- wildcard before token
select tokensForLikePattern('abc%', 'unicodeWord');            -- token before wildcard
select tokensForLikePattern('a_b%c_d', 'unicodeWord');         -- wildcard in between
select tokensForLikePattern('a\%b a\_c', 'unicodeWord');       -- escaped % breaks token (not alnum), escaped _ joins token
select tokensForLikePattern('a\_b%c', 'unicodeWord');          -- mix escaped _ and unescaped %
select tokensForLikePattern('a\_b\%c', 'unicodeWord');         -- all escaped wildcards
select tokensForLikePattern('_a c%', 'unicodeWord');           -- leading and trailing wildcards

select tokensForLikePattern($$a:b%cd\_e.f'g$$, 'unicodeWord'); -- mix colon, dot, single quote, wildcard, escaped _
select tokensForLikePattern('%%__abc', 'unicodeWord');         -- multiple leading wildcards
select tokensForLikePattern('', 'unicodeWord');                -- empty string
select tokensForLikePattern('%%__', 'unicodeWord');            -- only wildcards
select tokensForLikePattern('你', 'unicodeWord');              -- single Unicode char
select tokensForLikePattern('abc你好', 'unicodeWord');         -- ASCII then Unicode
select tokensForLikePattern('你好%世界', 'unicodeWord');       -- Unicode token with wildcards

set enable_analyzer = 1;
set enable_full_text_index = 1;

drop table if exists tab;

create table tab (key UInt64, str String, index text_idx(str) type text(tokenizer = unicodeWord)) engine MergeTree order by key;

insert into tab values (1, 'hello错误502需要处理kitty');

explain estimate select * from tab where str like '%错误502需要%';

explain estimate select * from tab where str like '%错误503需要%';

drop table tab;
