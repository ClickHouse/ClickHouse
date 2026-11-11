SELECT
    toTypeName(getSubcolumn(d, 'Tuple(UInt64, String).null')),
    getSubcolumn(d, 'Tuple(UInt64, String).null')
FROM (SELECT 42::Dynamic AS d); -- { serverError ILLEGAL_COLUMN }

SELECT
    toTypeName(getSubcolumn(d, 'Array(UInt64).null')),
    getSubcolumn(d, 'Array(UInt64).null')
FROM (SELECT 42::Dynamic AS d); -- { serverError ILLEGAL_COLUMN }

SELECT
    toTypeName(getSubcolumn(d, 'Map(String, UInt64).null')),
    getSubcolumn(d, 'Map(String, UInt64).null')
FROM (SELECT 42::Dynamic AS d); -- { serverError ILLEGAL_COLUMN }
