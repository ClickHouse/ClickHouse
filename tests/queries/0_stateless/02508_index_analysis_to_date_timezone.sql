DROP TABLE IF EXISTS table;
CREATE TABLE table (uid UUID, date DateTime('Asia/Kamchatka')) ENGINE = MergeTree ORDER BY date;

INSERT INTO `table` VALUES ('4c36abda-8bd8-11eb-8204-005056aa8bf6', '2021-03-24 01:04:27'), ('4c408902-8bd8-11eb-8204-005056aa8bf6', '2021-03-24 01:04:27'), ('4c5bf20a-8bd8-11eb-8204-005056aa8bf6', '2021-03-24 01:04:27'), ('4c61623a-8bd8-11eb-8204-005056aa8bf6', '2021-03-24 01:04:27'), ('4c6efab2-8bd8-11eb-a952-005056aa8bf6', '2021-03-24 01:04:27');

SELECT uid, date, toDate(date) = toDate('2021-03-24') AS res FROM table WHERE res = 1 ORDER BY uid, date;
SELECT '---';
SELECT uid, date, toDate(date) = toDate('2021-03-24') AS res FROM table WHERE toDate(date) = toDate('2021-03-24') ORDER BY uid, date;

DROP TABLE table;
