-- `MATERIALIZE INDEX <lookup_name>` must be rejected even with `validate_mutation_query = 0`:
-- the check lives in `checkMutationIsPossible` (an unconditional validation path), not only in
-- `MutationsInterpreter`, which is skipped in that mode. Otherwise the command would be accepted
-- and queued as a silent no-op mutation.

SET allow_experimental_lookup_index = 1;
SET validate_mutation_query = 0;

DROP TABLE IF EXISTS table_lookup_materialize_no_validation SYNC;

CREATE TABLE table_lookup_materialize_no_validation
(
    id UInt64,
    value String,
    LOOKUP INDEX idx_lookup (id) TYPE table_set
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO table_lookup_materialize_no_validation VALUES (1, 'a'), (2, 'b');

ALTER TABLE table_lookup_materialize_no_validation MATERIALIZE INDEX idx_lookup; -- { serverError BAD_ARGUMENTS }

SELECT count() FROM table_lookup_materialize_no_validation;

DROP TABLE table_lookup_materialize_no_validation SYNC;
