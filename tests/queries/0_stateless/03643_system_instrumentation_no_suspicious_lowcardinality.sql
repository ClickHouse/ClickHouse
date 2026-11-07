-- Tags: use-xray
-- Test that system.instrumentation.function_id is Int32, not LowCardinality(Int32)
-- This allows CREATE VIEW without allow_suspicious_low_cardinality_types setting

-- Verify the column type
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'instrumentation' AND name = 'function_id';

-- This should work without setting allow_suspicious_low_cardinality_types
DROP VIEW IF EXISTS test_instrumentation_view;
CREATE VIEW test_instrumentation_view AS
SELECT
    id,
    function_id,
    function_name,
    handler,
    entry_type,
    symbol,
    parameters
FROM system.instrumentation;

-- Verify the view was created
SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'test_instrumentation_view';

-- Clean up
DROP VIEW test_instrumentation_view;
