-- Tags: use-xray

SET allow_introspection_functions=1;

SELECT function_id > 0, length(symbol_demangled) > 10 FROM system.symbols WHERE symbol_demangled LIKE '%QueryMetricLog::startQuery%';
