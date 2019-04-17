-- this test cannot pass without the new DFA matching algorithm of sequenceMatch

DROP TABLE IF EXISTS sequence;

CREATE TABLE sequence
(
    userID UInt64,
    eventType Enum8('A' = 1, 'B' = 2, 'C' = 3),
    EventTime UInt64
)
ENGINE = Memory;

INSERT INTO sequence SELECT 1, number = 0 ? 'A' : (number < 1000000 ? 'B' : 'C'), number FROM numbers(1000001);

SELECT userID
FROM sequence
GROUP BY userID
HAVING sequenceMatch('(?1).*(?2).*(?3)')(toDateTime(EventTime), eventType = 'A', eventType = 'B', eventType = 'C');

SELECT userID
FROM sequence
GROUP BY userID
HAVING sequenceMatch('(?1).*(?2).*(?3)')(toDateTime(EventTime), eventType = 'A', eventType = 'B', eventType = 'A');

DROP TABLE sequence;
