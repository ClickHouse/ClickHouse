DROP TABLE IF EXISTS t_text_index_bug101314;
DROP TABLE IF EXISTS t_text_index_bug101314_legacy SYNC;

CREATE TABLE t_text_index_bug101314
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = CAST(val, 'FixedString(3)')
    )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }

SELECT 'Legacy unsafe text index loaded with ATTACH';

ATTACH TABLE t_text_index_bug101314_legacy UUID '00000000-0000-0000-0000-000000101314'
(
    id UInt64,
    val String,
    INDEX idx(val) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = CAST(val, 'FixedString(3)')
    )
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_text_index_bug101314_legacy VALUES (1, 'foo'), (2, 'bar'), (3, 'baz');

SELECT count() FROM t_text_index_bug101314_legacy WHERE hasAllTokens(val, 'foobar');
SELECT count() FROM t_text_index_bug101314_legacy WHERE val IN ('foo', 'foobar');

DROP TABLE t_text_index_bug101314_legacy SYNC;

CREATE TABLE t_text_index_bug101314_fixed_string
(
    id UInt64,
    val FixedString(10),
    INDEX idx(val) TYPE text(
        tokenizer = 'splitByNonAlpha',
        preprocessor = CAST(val, 'FixedString(3)')
    )
)
ENGINE = MergeTree
ORDER BY id; -- { serverError INCORRECT_QUERY }
