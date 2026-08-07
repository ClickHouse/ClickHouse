-- Tags: stateful
SELECT EventTime::DateTime('Asia/Dubai') FROM test.hits ORDER BY EventTime DESC LIMIT 10
