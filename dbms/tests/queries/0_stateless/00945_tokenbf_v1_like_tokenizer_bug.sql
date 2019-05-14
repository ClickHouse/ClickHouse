SET allow_experimental_data_skipping_indices = 1;

DROP TABLE IF EXISTS bloom_filter_like_tokenizer_bug;

CREATE TABLE bloom_filter_like_tokenizer_bug
(
    id UInt64,
    s String,
    INDEX tok_bf (s, lower(s)) TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8;

insert into bloom_filter_like_tokenizer_bug select number, 'yyy,uuu' from numbers(1024);
insert into bloom_filter_like_tokenizer_bug select number+2000, 'abc,def,zzz' from numbers(8);
insert into bloom_filter_like_tokenizer_bug select number+3000, 'yyy,uuu' from numbers(1024);

set max_rows_to_read = 16;

SELECT max(id) FROM bloom_filter_like_tokenizer_bug WHERE s like '%,def,%';

SELECT max(id) FROM bloom_filter_like_tokenizer_bug WHERE s like '%def%';

DROP TABLE bloom_filter_like_tokenizer_bug;
