-- Tags: stateful, distributed


SELECT anyIf(SearchPhrase, CounterID = -1) FROM remote('127.0.0.{1,2}:9000', test, hits)
