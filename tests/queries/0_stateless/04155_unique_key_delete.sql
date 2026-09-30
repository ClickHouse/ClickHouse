-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-fasttest
-- UNIQUE KEY: a DELETE inside a transaction is refused and changes nothing.

SET enable_unique_key = 1;

-- Red if a UNIQUE KEY write stops refusing an explicit transaction (the DELETE then nests in
-- the implicit transaction and aborts the debug server).
DROP TABLE IF EXISTS uk_guard;

CREATE TABLE uk_guard (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id);

SYSTEM STOP MERGES uk_guard;

INSERT INTO uk_guard VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e');

DELETE FROM uk_guard WHERE id % 2 = 0;  -- removes 2, 4

-- NOT_IMPLEMENTED where transactions are off (the default), SUPPORT_IS_DISABLED where they are on.
DELETE FROM uk_guard WHERE id = 1 SETTINGS implicit_transaction = 1; -- { serverError SUPPORT_IS_DISABLED, NOT_IMPLEMENTED }

SELECT 'survivors_unchanged' AS step, id FROM uk_guard ORDER BY id;  -- 1,3,5

DROP TABLE uk_guard;
