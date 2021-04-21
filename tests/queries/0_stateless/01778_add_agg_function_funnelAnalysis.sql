DROP TABLE IF EXISTS testtbl;

CREATE TABLE testtbl (`user_id` String, `event` String, timestamps Array(UInt64))
ENGINE = MergeTree() ORDER BY (event, user_id);

INSERT INTO testtbl VALUES('1', 'event_1', [1611239038,1611239100]);
INSERT INTO testtbl VALUES('1', 'event_2', [1611239125,1611239265]);
INSERT INTO testtbl VALUES('1', 'event_3', [1611239261]);

INSERT INTO testtbl VALUES('2', 'event_1', [1611240011]);
INSERT INTO testtbl VALUES('2', 'event_2', [1611240112,1611241220]);


SELECT user_id, funnelAnalysis(3600, 0, 0, 'event_1', 'event_2', 'event_3')(event, timestamps) AS level
FROM remote('localhost,127.0.0.1',currentDatabase(),'testtbl')
WHERE event IN ('event_1', 'event_2', 'event_3')
GROUP BY user_id
ORDER BY user_id;

DROP TABLE testtbl;
