SELECT 
    materialize(toLowCardinality('')) AS lc,
    toTypeName(lc)
WHERE lc = defaultValueOfArgumentType(lc)
