SELECT map('a', 1, 'b', 2) FROM remote('127.0.0.{1,2}', system, one);
