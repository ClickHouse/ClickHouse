SELECT demangle(symbol) AS x FROM system.symbols WHERE symbol LIKE '%StorageSystemSymbols%' ORDER BY x LIMIT 1 SETTINGS allow_introspection_functions = 1;
