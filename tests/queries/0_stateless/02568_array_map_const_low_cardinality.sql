SELECT arrayMap(x -> (toLowCardinality(1) + 1::Nullable(UInt8)), [1]);
