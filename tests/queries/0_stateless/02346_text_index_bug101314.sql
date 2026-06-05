DROP TABLE IF EXISTS t_text_index_bug101314;

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
