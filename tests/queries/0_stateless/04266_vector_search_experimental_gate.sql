-- Tags: no-fasttest

-- The `vectorSearch` table function is gated behind
-- `allow_experimental_search_topk_table_functions`. Default off → SUPPORT_IS_DISABLED.
-- Explicit on → the gate is passed and a downstream resolution error surfaces (here:
-- UNKNOWN_TABLE for a non-existent source), proving the gate did not swallow the call.

-- 1. Default off: setting must be explicitly enabled.
SELECT 1 FROM vectorSearch(currentDatabase(), no_such_table_xxxxxx, [0.1, 0.2, 0.3]::Array(Float32), 1)
SETTINGS allow_experimental_search_topk_table_functions = 0; -- { serverError SUPPORT_IS_DISABLED }

-- 2. Explicit on: the gate is passed; the call surfaces UNKNOWN_TABLE for the missing source.
SELECT 1 FROM vectorSearch(currentDatabase(), no_such_table_xxxxxx, [0.1, 0.2, 0.3]::Array(Float32), 1)
SETTINGS allow_experimental_search_topk_table_functions = 1; -- { serverError UNKNOWN_TABLE }
