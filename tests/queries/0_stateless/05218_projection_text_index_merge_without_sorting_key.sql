-- A table without a sorting key merges its parts by concatenation, so the merged `_part_offset` of a row is its
-- offset in the source part plus the starting offset of that part. The projection text index maps its posting lists
-- through `MergedPartOffsets` in this mode; the mapping used to be built without the starting offsets and the merge
-- failed with a logical error (or wrote wrong offsets in the release build).

SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

DROP TABLE IF EXISTS tab_no_sorting_key;

CREATE TABLE tab_no_sorting_key
(
    id UInt64,
    s String,
    PROJECTION text_proj INDEX s TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO tab_no_sorting_key SELECT number, concat('first part token', toString(number)) FROM numbers(10);
INSERT INTO tab_no_sorting_key SELECT 100 + number, concat('second part token', toString(100 + number)) FROM numbers(10);
INSERT INTO tab_no_sorting_key SELECT 200 + number, concat('third part token', toString(200 + number)) FROM numbers(10);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_no_sorting_key' AND active;

OPTIMIZE TABLE tab_no_sorting_key FINAL;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_no_sorting_key' AND active;

-- The merged projection must point at the right rows of the merged part.
SELECT id, s FROM tab_no_sorting_key WHERE hasToken(s, 'second') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id, s FROM tab_no_sorting_key WHERE hasToken(s, 'token205') ORDER BY id SETTINGS use_skip_indexes = 1;
SELECT id FROM tab_no_sorting_key WHERE hasAnyTokens(s, ['token3', 'token103', 'token203']) ORDER BY id SETTINGS use_skip_indexes = 1;

-- Cross-check against a plain scan.
SELECT
    (SELECT arraySort(groupArray(id)) FROM tab_no_sorting_key WHERE hasToken(s, 'third') SETTINGS use_skip_indexes = 1)
    = (SELECT arraySort(groupArray(id)) FROM tab_no_sorting_key WHERE hasToken(s, 'third') SETTINGS use_skip_indexes = 0);

DROP TABLE tab_no_sorting_key;
