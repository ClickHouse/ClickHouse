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

SELECT tokensForLikePattern('%abc', 'standard');            -- wildcard before token
SELECT tokensForLikePattern('abc%', 'standard');            -- token before wildcard
SELECT tokensForLikePattern('a_b%c_d', 'standard');         -- wildcard in between
SELECT tokensForLikePattern('a\%b a\_c', 'standard');       -- escaped % and _ included as normal chars
SELECT tokensForLikePattern('a\_b%c', 'standard');          -- mix escaped _ and unescaped %
SELECT tokensForLikePattern('a\_b\%c', 'standard');         -- all escaped wildcards
SELECT tokensForLikePattern('_a c%', 'standard');           -- leading and trailing wildcards

SELECT tokensForLikePattern($$a:b%cd\_e.f'g$$, 'standard'); -- mix colon, dot, single quote, wildcard, escaped _
SELECT tokensForLikePattern('%%__abc', 'standard');         -- multiple leading wildcards
SELECT tokensForLikePattern('', 'standard');                -- empty string
SELECT tokensForLikePattern('%%__', 'standard');            -- only wildcards
SELECT tokensForLikePattern('你', 'standard');              -- single Unicode char
SELECT tokensForLikePattern('abc你好', 'standard');         -- ASCII then Unicode
SELECT tokensForLikePattern('，。你好', 'standard');        -- punctuation stop words skipped
SELECT tokensForLikePattern('你好%世界', 'standard');       -- Unicode token with wildcards

set allow_experimental_full_text_index = 1;

drop table if exists tab;

create table tab (key UInt64, str String, index text_idx(str) type text(tokenizer = standard)) engine MergeTree order by key;

insert into tab values (1, 'hello错误502需要处理kitty');

explain estimate select * from tab where str like '%错误502需要%';

explain estimate select * from tab where str like '%错误503需要%';

drop table tab;
