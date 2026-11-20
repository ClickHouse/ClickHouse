-- A full-definition `ATTACH TABLE t (...) ENGINE = ...` is fresh user input (it runs under
-- `LoadingStrictnessLevel::ATTACH`, but the definition comes from the query, not from stored metadata),
-- so a `CHECK` constraint containing a subquery must be rejected there exactly like on `CREATE TABLE`.

ATTACH TABLE attach_full_check_bare (c0 Int, CONSTRAINT c0 CHECK (SELECT 1)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE attach_full_check_scalar (c0 Int, CONSTRAINT c0 CHECK equals((SELECT 1), c0)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
ATTACH TABLE attach_full_check_in_lhs (c0 Int, CONSTRAINT c0 CHECK (SELECT 1) IN (1, 2, 3)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
-- The `UUID` form is the one an `Atomic` database accepts; the constraint is rejected before the table
-- (or its UUID) is registered anywhere, so nothing is persisted.
ATTACH TABLE attach_full_check_uuid UUID '11112222-3333-4444-5555-666677778888' (c0 Int, CONSTRAINT c0 CHECK equals((SELECT 1), c0)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'attach_full_check_%';

-- The short `ATTACH TABLE t` form reads back the stored metadata and must not be validated again,
-- otherwise a table created before this restriction existed would stop loading.
CREATE TABLE attach_short (c0 Int, CONSTRAINT c0 CHECK c0 > 0) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO attach_short VALUES (1);
DETACH TABLE attach_short;
ATTACH TABLE attach_short;
SELECT count() FROM attach_short;
INSERT INTO attach_short VALUES (-1); -- { serverError VIOLATED_CONSTRAINT }
