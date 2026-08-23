SET allow_experimental_lookup_index = 1;

CREATE TABLE lookup_index_auto_minmax_reserved
(
    id UInt64,
    LOOKUP INDEX auto_minmax_index_id (id) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1; -- { serverError BAD_ARGUMENTS }

CREATE TABLE lookup_index_auto_minmax_reserved
(
    id UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS add_minmax_index_for_numeric_columns = 1;

-- The implicit minmax index for `id` already occupies this name.
ALTER TABLE lookup_index_auto_minmax_reserved
    ADD LOOKUP INDEX auto_minmax_index_id (id) TYPE table_set; -- { serverError ILLEGAL_COLUMN }

-- A reserved name that does not clash with an existing implicit index is still rejected.
ALTER TABLE lookup_index_auto_minmax_reserved
    ADD LOOKUP INDEX auto_minmax_index_absent (id) TYPE table_set; -- { serverError BAD_ARGUMENTS }

DROP TABLE lookup_index_auto_minmax_reserved SYNC;
