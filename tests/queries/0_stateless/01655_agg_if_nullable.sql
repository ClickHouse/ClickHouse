SELECT sumIf(toNullable(1), 1) FROM remote('127.0.0.{1,2}', system.one);
