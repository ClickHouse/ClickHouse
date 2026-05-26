SELECT countIf(dummy IN [1, -1]) from remote('127.0.0.{1,2}', 'system', 'one');
