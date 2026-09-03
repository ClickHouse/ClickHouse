-- Tags: global

SELECT 1 GLOBAL IN (SELECT 1), 2 GLOBAL IN (SELECT 2) FROM remote('localhost', system.one);
