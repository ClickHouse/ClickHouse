ATTACH TABLE sophisticated_default
(
    a UInt8 DEFAULT
    (
        SELECT number FROM system.numbers LIMIT 3,1
    ),
    b UInt8 ALIAS
    (
        SELECT dummy+9 FROM system.one
    ),
    c UInt8
) ENGINE = Memory
