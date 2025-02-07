-- Tags: stateful
select 1 IN (1, 2, 3);

SELECT count() FROM remote('localhost', test, hits) WHERE CounterID IN (598875);
