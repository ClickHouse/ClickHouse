SELECT toTypeName(getSubcolumn(tuple('str'::LowCardinality(String) as a), 'a'))
