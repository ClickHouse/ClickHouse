SELECT 1 GLOBAL IN (SELECT 1), 2 GLOBAL IN (SELECT 2) FROM remote('localhost,127.0.0.2', system.one);
