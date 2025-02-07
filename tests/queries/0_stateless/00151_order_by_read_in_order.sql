-- Tags: stateful
SET optimize_read_in_order = 1;
SELECT CounterID FROM test.hits ORDER BY CounterID DESC LIMIT 50;
SELECT CounterID FROM test.hits ORDER BY CounterID LIMIT 50;
SELECT CounterID FROM test.hits ORDER BY CounterID, EventDate LIMIT 50;
SELECT EventDate FROM test.hits ORDER BY CounterID, EventDate LIMIT 50;
SELECT EventDate FROM test.hits ORDER BY CounterID, EventDate DESC LIMIT 50;
SELECT CounterID FROM test.hits ORDER BY CounterID, EventDate DESC LIMIT 50;
SELECT CounterID FROM test.hits ORDER BY CounterID DESC, EventDate DESC LIMIT 50;
SELECT EventDate FROM test.hits ORDER BY CounterID DESC, EventDate DESC LIMIT 50;

SELECT CounterID, EventDate FROM test.hits ORDER BY CounterID, EventDate LIMIT 50;
SELECT CounterID, EventDate FROM test.hits ORDER BY CounterID, EventDate DESC LIMIT 50;
SELECT CounterID, EventDate FROM test.hits ORDER BY CounterID DESC, EventDate LIMIT 50;
SELECT CounterID, EventDate FROM test.hits ORDER BY CounterID DESC, EventDate DESC LIMIT 50;
