-- Tags: no-ordinary-database, no-replicated-database, no-shared-merge-tree, no-object-storage, no-s3-storage
-- UNIQUE KEY DDL + metadata.
-- Runtime dedup is out of scope; this test only exercises parsing, metadata,
-- round-trip, guards, and restart survival.

SET allow_experimental_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_t;
DROP TABLE IF EXISTS uk_t_src;
DROP TABLE IF EXISTS uk_t_other;
DROP TABLE IF EXISTS uk_t_rt;
DROP TABLE IF EXISTS uk_t_plain;
DROP TABLE IF EXISTS uk_t_composite;

-- 1. CREATE TABLE with UNIQUE KEY succeeds under the experimental gate.
CREATE TABLE uk_t (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id);

-- 2. SHOW CREATE emits UNIQUE KEY (...) and round-trips.
SHOW CREATE TABLE uk_t FORMAT TSVRaw;

-- 3. system.tables.unique_key is populated.
SELECT unique_key FROM system.tables WHERE database = currentDatabase() AND name = 'uk_t';

-- 4. Same CREATE fails without the experimental setting.
DROP TABLE uk_t;

SET allow_experimental_unique_key = 0;

CREATE TABLE uk_t (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id); -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_unique_key = 1;

-- 5. UNIQUE KEY not a prefix of ORDER BY: supported via the per-part
-- dense-index SST, which gives probes efficient point-lookup independent
-- of sort order.
CREATE TABLE uk_t_nonprefix (id UInt64, user_id UInt32)
ENGINE = MergeTree
UNIQUE KEY (user_id)
ORDER BY (id, user_id);

DROP TABLE uk_t_nonprefix;

-- 6. UNIQUE KEY on non-MergeTree engine -> error.
CREATE TABLE uk_t (id UInt64, v String)
ENGINE = Log
UNIQUE KEY (id); -- { serverError BAD_ARGUMENTS }

-- 6a. UNIQUE KEY on Replicated*MergeTree -> error.
-- ReplicatedMergeTreeTableMetadata does not yet serialize `unique_key`, so
-- replicas could diverge silently. The supports_unique_key feature flag is
-- set only on the non-replicated variants.
CREATE TABLE uk_t (id UInt64, v String)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/04046_uk_repl', 'r1')
UNIQUE KEY (id)
ORDER BY (id); -- { serverError BAD_ARGUMENTS }

-- 7. UNIQUE KEY referring to non-existent column -> error.
CREATE TABLE uk_t (id UInt64, user_id UInt32)
ENGINE = MergeTree
UNIQUE KEY (does_not_exist)
ORDER BY (id, user_id); -- { serverError UNKNOWN_IDENTIFIER }

-- 7a. Expression-style UNIQUE KEY elements are rejected at DDL time.
-- Function call as a single-element key.
CREATE TABLE uk_t (ts DateTime, v String)
ENGINE = MergeTree
UNIQUE KEY (toDate(ts))
ORDER BY (ts); -- { serverError BAD_ARGUMENTS }

-- 7b. Function call inside a tuple element.
CREATE TABLE uk_t (ts DateTime, id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id, toDate(ts))
ORDER BY (id, ts); -- { serverError BAD_ARGUMENTS }

-- 7c. Literal as a UNIQUE KEY element.
CREATE TABLE uk_t (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (1)
ORDER BY (id); -- { serverError BAD_ARGUMENTS }

-- 7d. Duplicate columns in a UNIQUE KEY are rejected.
CREATE TABLE uk_t (a Int, b Int)
ENGINE = MergeTree
UNIQUE KEY (a, a)
ORDER BY (b); -- { serverError BAD_ARGUMENTS }

-- 8. ALTER DROP COLUMN on a unique-key column -> error (via ORDER BY key guard).
CREATE TABLE uk_t (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id);

-- The dedicated UNIQUE KEY guard in `checkAlterIsPossible` intercepts BEFORE
-- the sort-key check, so the error is `ALTER_OF_COLUMN_IS_FORBIDDEN` (524),
-- not `UNKNOWN_IDENTIFIER` (47).
ALTER TABLE uk_t DROP COLUMN id; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- 9. ALTER RENAME COLUMN on a unique-key column -> error.
ALTER TABLE uk_t RENAME COLUMN id TO id2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

-- 10. ALTER ADD PROJECTION on a unique-key table -> error.
ALTER TABLE uk_t ADD PROJECTION p (SELECT id, user_id); -- { serverError SUPPORT_IS_DISABLED }

-- 11. ALTER MODIFY ORDER BY on a unique-key table -> error.
ALTER TABLE uk_t MODIFY ORDER BY (id); -- { serverError SUPPORT_IS_DISABLED }

-- 12. INSERT ... SETTINGS async_insert = 1 on a unique-key table — allowed.

-- 13. ALTER DELETE / ALTER UPDATE on a unique-key table -> error.
ALTER TABLE uk_t DELETE WHERE id = 1; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_t UPDATE v = 'x' WHERE id = 1; -- { serverError SUPPORT_IS_DISABLED }

-- 14. All ALTER ... PARTITION operations are blocked on UK tables.
CREATE TABLE uk_t_src (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id)
PARTITION BY user_id;

INSERT INTO uk_t_src VALUES (1, 10, 'a');

CREATE TABLE uk_t_dst (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id)
PARTITION BY user_id;

ALTER TABLE uk_t_dst ATTACH PARTITION 10 FROM uk_t_src; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_t_dst REPLACE PARTITION 10 FROM uk_t_src; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_t_src DROP PARTITION 10;                  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_t_src DETACH PARTITION 10;                -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE uk_t_src FREEZE PARTITION 10;                -- { serverError SUPPORT_IS_DISABLED }

-- 15. MOVE PARTITION TO TABLE -> error.
CREATE TABLE uk_t_other (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id)
PARTITION BY user_id;

ALTER TABLE uk_t_src MOVE PARTITION 10 TO TABLE uk_t_other; -- { serverError SUPPORT_IS_DISABLED }

-- 16. Round-trip survival across DETACH/ATTACH (stand-in for restart).
CREATE TABLE uk_t_rt (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id);

DETACH TABLE uk_t_rt;
ATTACH TABLE uk_t_rt;

SHOW CREATE TABLE uk_t_rt FORMAT TSVRaw;
SELECT unique_key FROM system.tables WHERE database = currentDatabase() AND name = 'uk_t_rt';

-- 17. Plain MergeTree without UNIQUE KEY is unaffected: unique_key column is empty.
CREATE TABLE uk_t_plain (id UInt64) ENGINE = MergeTree ORDER BY id;
SELECT unique_key FROM system.tables WHERE database = currentDatabase() AND name = 'uk_t_plain';

-- 18. INSERT (sync) into a unique-key table succeeds (no dedup probe asserted here).
INSERT INTO uk_t VALUES (1, 10, 'a'), (2, 20, 'b');
SELECT count() FROM uk_t;

-- 19. Composite UNIQUE KEY as ORDER BY prefix works.
CREATE TABLE uk_t_composite (id UInt64, sub UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id, sub)
ORDER BY (id, sub);

SHOW CREATE TABLE uk_t_composite FORMAT TSVRaw;

-- 20. Regression: inline PRIMARY KEY in column definition + UNIQUE KEY.
-- ParserCreateQuery hoists the inline PRIMARY KEY into storage->primary_key
-- then calls ASTStorage::normalizeChildrenOrder() — which historically
-- dropped `unique_key` from the children vector, causing a use-after-free
-- on subsequent access (surfaced as a spurious "Missing columns" error
-- reading freed memory, or a heap-use-after-free under ASan).
DROP TABLE IF EXISTS uk_inline_pk;
CREATE TABLE uk_inline_pk (id UInt64 PRIMARY KEY, v String)
ENGINE = MergeTree UNIQUE KEY (id);

SHOW CREATE TABLE uk_inline_pk FORMAT TSVRaw;
SELECT unique_key FROM system.tables WHERE database = currentDatabase() AND name = 'uk_inline_pk';

-- 21. ATTACH/REPLACE PARTITION FROM a UK source -> plain destination is blocked.
DROP TABLE IF EXISTS uk_src_for_plain;
DROP TABLE IF EXISTS plain_dst_from_uk;
CREATE TABLE uk_src_for_plain (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id, user_id)
PARTITION BY user_id;

CREATE TABLE plain_dst_from_uk (id UInt64, user_id UInt32, v String)
ENGINE = MergeTree
ORDER BY (id, user_id)
PARTITION BY user_id;

INSERT INTO uk_src_for_plain VALUES (1, 10, 'a');
ALTER TABLE plain_dst_from_uk ATTACH PARTITION 10 FROM uk_src_for_plain;  -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE plain_dst_from_uk REPLACE PARTITION 10 FROM uk_src_for_plain; -- { serverError SUPPORT_IS_DISABLED }

DROP TABLE uk_t;
DROP TABLE uk_t_src;
DROP TABLE uk_t_other;
DROP TABLE uk_t_dst;
DROP TABLE uk_t_rt;
DROP TABLE uk_t_plain;
DROP TABLE uk_t_composite;
DROP TABLE uk_inline_pk;
DROP TABLE uk_src_for_plain;
DROP TABLE plain_dst_from_uk;
