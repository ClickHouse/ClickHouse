CREATE TABLE t0 (c0 String) ENGINE = CoalescingMergeTree() ORDER BY tuple();
INSERT INTO TABLE t0 (c0) VALUES ('playl哪国人[]美国认识你很高兴');
SELECT * FROM t0;
