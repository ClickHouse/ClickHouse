-- { echoOn }
-- ascii
select tokens('a_b a_3 a_ _a __a_b_3_', 'unicode_word');
select tokens('3_b 3_3 3_ _3 __3_4_3_', 'unicode_word');
select tokens('___ _3___a_ _a__a_b_c_ ___', 'unicode_word');

select tokens('a:b a:3 a: :a ::a:b:3:', 'unicode_word');
select tokens('3:b 3:3 3: :3 ::3:4:3:', 'unicode_word');
select tokens('::: :3:::a: :a::a:b:c: :::', 'unicode_word');

select tokens($$a'b a'3 a' 'a ''a'b'3'$$, 'unicode_word');
select tokens($$3'b 3'3 3' '3 ''3'4'3'$$, 'unicode_word');
select tokens($$''' '3'''a' 'a''a'b'c' '''$$, 'unicode_word');

select tokens($$a.b a.3 a. .a ..a.b.3.$$, 'unicode_word');
select tokens($$3.b 3.3 3. .3 ..3.4.3.$$, 'unicode_word');
select tokens($$... .3...a. .a..a.b.c. ...$$, 'unicode_word');

-- ascii and Chinese
select tokens('错误503', 'unicode_word');
select tokens('taichi张三丰in the house', 'unicode_word');

-- stop words
select tokens('错误，503', 'unicode_word'); -- default stop words contains common full-width separators
select tokens('错误and 503', 'unicode_word', ['and']);

-- edge cases
select tokens('', 'unicode_word');                               -- empty string
select tokens('a.b', 'unicode_word', ['.']);                     -- dot as stop word does not suppress connector
select tokens('a.b', 'unicode_word', ['a.b']);                   -- dot-connected token as stop word
select tokens('test', 'unicode_word', 'not_an_array'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT } -- wrong stop_words type

select tokensForLikePattern('%abc', 'unicode_word');            -- wildcard before token
select tokensForLikePattern('abc%', 'unicode_word');            -- token before wildcard
select tokensForLikePattern('a_b%c_d', 'unicode_word');         -- wildcard in between
select tokensForLikePattern('a\%b a\_c', 'unicode_word');       -- escaped % breaks token (not alnum), escaped _ joins token
select tokensForLikePattern('a\_b%c', 'unicode_word');          -- mix escaped _ and unescaped %
select tokensForLikePattern('a\_b\%c', 'unicode_word');         -- all escaped wildcards
select tokensForLikePattern('_a c%', 'unicode_word');           -- leading and trailing wildcards

select tokensForLikePattern($$a:b%cd\_e.f'g$$, 'unicode_word'); -- mix colon, dot, single quote, wildcard, escaped _
select tokensForLikePattern('%%__abc', 'unicode_word');         -- multiple leading wildcards
select tokensForLikePattern('', 'unicode_word');                -- empty string
select tokensForLikePattern('%%__', 'unicode_word');            -- only wildcards
select tokensForLikePattern('你', 'unicode_word');              -- single Unicode char
select tokensForLikePattern('abc你好', 'unicode_word');         -- ASCII then Unicode
select tokensForLikePattern('，。你好', 'unicode_word');        -- punctuation stop words skipped
select tokensForLikePattern('你好%世界', 'unicode_word');       -- Unicode token with wildcards

set enable_analyzer = 1;
set enable_full_text_index = 1;

drop table if exists tab;

create table tab (key UInt64, str String, index text_idx(str) type text(tokenizer = unicode_word)) engine MergeTree order by key;

insert into tab values (1, 'hello错误502需要处理kitty');

explain estimate select * from tab where str like '%错误502需要%';

explain estimate select * from tab where str like '%错误503需要%';

drop table tab;
