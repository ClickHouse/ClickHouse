SELECT toTypeName(getSubcolumn(tuple('str')::Tuple(a LowCardinality(String)), 'a'))
