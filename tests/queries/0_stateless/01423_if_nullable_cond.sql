SELECT CAST(null, 'Nullable(UInt8)') = 1 ? CAST(null, 'Nullable(UInt8)') : -1 AS x, toTypeName(x), dumpColumnStructure(x);
