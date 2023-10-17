ATTACH TABLE sophisticated_default
(
    a UInt8 DEFAULT 3,
    b UInt8 ALIAS a - 3 + 9,
    c UInt8
) ENGINE = Memory
