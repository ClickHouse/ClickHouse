-- Tags: no-fasttest

SET allow_experimental_row_type = 1;

DROP TABLE IF EXISTS row_element_index;

CREATE TABLE row_element_index (
    id UInt64,
    combined Row(a String, b UInt32) MATERIALIZED (toString(id), toUInt32(id * 10))
) ENGINE = MergeTree ORDER BY id;

INSERT INTO row_element_index (id) VALUES (1), (2);

-- Constant index and constant name work.
SELECT __rowElement(combined, 1), __rowElement(combined, 2), __rowElement(combined, 'b') FROM row_element_index ORDER BY id;

-- A non-constant index must be rejected during type resolution instead of dereferencing a missing column.
SELECT __rowElement(combined, id) FROM row_element_index; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __rowElement(combined, materialize(1)) FROM row_element_index; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __rowElement(combined, NULL) FROM row_element_index; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __rowElement(combined, 0) FROM row_element_index; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT __rowElement(combined, 3) FROM row_element_index; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE row_element_index;
