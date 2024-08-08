SELECT
    JSONExtract('{"a":1, "b":"test"}', 'Tuple(a UInt8, b String)') AS x,
    x.a
SETTINGS enable_analyzer = 1;
