SELECT CAST([('a', 1), ('b', 2)], 'Map(String, UInt8)');
SELECT CAST((('a', 1), ('b', 2)), 'Map(String, UInt8)'); -- { serverError TYPE_MISMATCH }
