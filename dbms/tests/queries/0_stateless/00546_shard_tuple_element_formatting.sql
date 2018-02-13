SELECT tupleElement((1, 2), toUInt8(1 + 0)) FROM remote('127.0.0.{1,2}', system.one);
