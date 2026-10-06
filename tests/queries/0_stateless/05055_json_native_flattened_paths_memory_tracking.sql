-- The FLATTENED (version 3) path list is sized from an untrusted count in the Native prefix, so its
-- container must be memory-tracked like the non-flat sibling (version 2) below.
-- Frame: 1 column, 1 row, name "j", type "JSON", 8-byte LE structure version, VarUInt path count 1000000.
SELECT * FROM format(Native, 'j JSON', unhex('0101016A044A534F4E0300000000000000C0843D')) SETTINGS max_memory_usage = 10000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT * FROM format(Native, 'j JSON', unhex('0101016A044A534F4E0200000000000000C0843D')) SETTINGS max_memory_usage = 10000000; -- { serverError MEMORY_LIMIT_EXCEEDED }
