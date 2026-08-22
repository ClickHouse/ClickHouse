SELECT identity(_CAST('x', 'Variant(LowCardinality(String), UInt64)')) AS x, toTypeName(x), variantType(x);
