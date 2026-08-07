SELECT has([(1, 2), (3, 4)], (toUInt16(3), 4));
SELECT hasAny([(1, 2), (3, 4)], [(toUInt16(3), 4)]);
SELECT hasAll([(1, 2), (3, 4)], [(toNullable(1), toUInt64(2)), (toUInt16(3), 4)]);
