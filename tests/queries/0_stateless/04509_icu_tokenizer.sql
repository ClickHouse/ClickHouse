-- Tags: no-fasttest, no-parallel-replicas
-- no-fasttest: the 'icu' tokenizer is only registered when built with ICU

-- The 'icu' tokenizer performs locale-aware Unicode word segmentation (UAX #29 plus
-- dictionary-based breaking). The locale is passed as the tokenizer-specific argument, e.g.
-- tokens(str, 'icu', 'ja') or, in a text index, tokenizer = icu('ja').

-- { echoOn }
-- English: whitespace/punctuation are dropped, words and numbers are kept
select tokens('The quick, brown fox!', 'icu', 'en');
select tokens('user@example.com sent 3.14 and 1,000', 'icu', 'en');

-- Chinese: dictionary-based segmentation of text without spaces
select tokens('我是一个中国人', 'icu', 'zh');
select tokens('北京欢迎你', 'icu', 'zh');
select tokens('错误503需要处理', 'icu', 'zh');

-- Japanese: mixed kanji / hiragana / katakana
select tokens('日本語の形態素解析', 'icu', 'ja');
select tokens('カタカナとひらがな', 'icu', 'ja');

-- Thai: no spaces between words
select tokens('สวัสดีครับ', 'icu', 'th');

-- Mixed scripts in one string
select tokens('taichi張三丰in the house', 'icu', 'en');

-- edge cases
select tokens('', 'icu', 'en');                    -- empty string

-- locale is mandatory and must not be empty
select tokens('abc', 'icu');                       -- { serverError BAD_ARGUMENTS }
select tokens('abc', 'icu', '');                   -- { serverError BAD_ARGUMENTS }

-- LIKE pattern tokenization is not supported for this tokenizer
select tokensForLikePattern('abc', 'icu', 'en');   -- { serverError BAD_ARGUMENTS }

-- text index integration
set enable_analyzer = 1;
set enable_full_text_index = 1;

drop table if exists tab;
create table tab (key UInt64, str String, index text_idx(str) type text(tokenizer = icu('zh'))) engine MergeTree order by key;
insert into tab values (1, '北京欢迎你');

-- The needle is tokenized with the same 'icu' tokenizer so it matches the index tokens.
explain estimate select * from tab where hasAnyTokens(str, tokens('北京', 'icu', 'zh'));
explain estimate select * from tab where hasAnyTokens(str, tokens('上海', 'icu', 'zh'));

drop table tab;
-- { echoOff }
