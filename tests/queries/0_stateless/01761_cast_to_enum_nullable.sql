SELECT toUInt8(assumeNotNull(cast(cast(NULL, 'Nullable(String)'), 'Nullable(Enum8(\'Hello\' = 1))')));
