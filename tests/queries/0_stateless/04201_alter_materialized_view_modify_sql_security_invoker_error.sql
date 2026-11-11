-- Test: ALTER TABLE <materialized view> MODIFY SQL SECURITY INVOKER must throw
-- QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW because INVOKER is not allowed for MVs.
-- Covers: src/Storages/StorageMaterializedView.cpp:745-746
--   StorageMaterializedView::checkAlterIsPossible
--     if (command.sql_security->as<ASTSQLSecurity &>().type == SQLSecurityType::INVOKER)
--         throw Exception(...QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW...);
-- The CREATE-time check at line 122-123 is tested by 02884, but the ALTER-time
-- check at 745-746 is NOT exercised in 0_stateless or integration tests.

DROP TABLE IF EXISTS gap04201_mv;
DROP TABLE IF EXISTS gap04201_src;

CREATE TABLE gap04201_src (x UInt64) ENGINE = MergeTree ORDER BY x;

CREATE MATERIALIZED VIEW gap04201_mv
ENGINE = MergeTree ORDER BY x
SQL SECURITY DEFINER
AS SELECT * FROM gap04201_src;

-- ALTER MODIFY SQL SECURITY INVOKER must fail on a materialized view.
ALTER TABLE gap04201_mv MODIFY SQL SECURITY INVOKER; -- { serverError QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW }

-- Sanity: SQL SECURITY remains DEFINER (the failed alter must not have changed it).
SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'gap04201_mv'
    AND create_table_query LIKE '%SQL SECURITY DEFINER%';

-- ALTER MODIFY SQL SECURITY NONE/DEFINER must still work for MV (positive controls).
ALTER TABLE gap04201_mv MODIFY SQL SECURITY NONE;
ALTER TABLE gap04201_mv MODIFY SQL SECURITY DEFINER;

DROP TABLE gap04201_mv;
DROP TABLE gap04201_src;
