-- Tags: use-xray, no-parallel:xray
-- Tag no-parallel: all `use-xray` tests share process-global XRay instrumentation state.

SET allow_introspection_functions=1;

SELECT count() > 0 FROM system.symbols WHERE symbol_demangled LIKE '%QueryMetricLog::startQuery%' AND function_id > 0 AND length(symbol_demangled) > 10;
