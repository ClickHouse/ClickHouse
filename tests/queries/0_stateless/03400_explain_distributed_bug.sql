set enable_analyzer=1, prefer_localhost_replica=1;

set serialize_query_plan=0;
explain distributed=1 SELECT * FROM remote('127.0.0.{1,2}', numbers_mt(1e6)) GROUP BY number ORDER BY number DESC LIMIT 10;

set serialize_query_plan=1;
explain distributed=1 SELECT * FROM remote('127.0.0.{1,2}', numbers_mt(1e6)) GROUP BY number ORDER BY number DESC LIMIT 10;
