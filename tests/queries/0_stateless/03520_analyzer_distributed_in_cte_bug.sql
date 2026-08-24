set enable_analyzer=1;
WITH a AS (SELECT dummy FROM remote('127.0.0.{3,2}', system.one)) SELECT sum(dummy IN (a)) FROM remote('127.0.0.{3,2}', system.one);
WITH a AS (SELECT dummy FROM remote('127.0.0.{1,2}', system.one)) SELECT sum(dummy IN (a)) FROM remote('127.0.0.{1,2}', system.one);
WITH a AS (SELECT dummy FROM remote('127.0.0.{1,2}', system.one)) SELECT sum(dummy IN (a)) FROM remote('127.0.0.{3,2}', system.one);
WITH a AS (SELECT dummy FROM remote('127.0.0.{3,2}', system.one)) SELECT sum(dummy IN (a)) FROM remote('127.0.0.{1,2}', system.one);
