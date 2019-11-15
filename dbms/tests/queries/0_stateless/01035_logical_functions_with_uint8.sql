select toUInt8(number) as n from numbers(100) where bitAnd(n, 1) and bitAnd(n, 2) and bitAnd(n, 4);
