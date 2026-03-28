-- Tags: stateful, replica, distributed, no-random-settings


SET max_parallel_replicas = 2;
SELECT EventTime::DateTime('Asia/Dubai') FROM remote('127.0.0.{1|2}', test, hits) ORDER BY EventTime DESC LIMIT 10
