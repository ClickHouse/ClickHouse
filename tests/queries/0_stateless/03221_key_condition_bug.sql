CREATE TABLE IF NOT EXISTS report_metrics_v2
(
	`a` UInt64
) Engine = MergeTree()
ORDER BY a;

insert into report_metrics_v2 SELECT * FROM system.numbers LIMIT 50000;

SELECT count(*) from report_metrics_v2 WHERE (intDiv(a, 50) = 200) AND (intDiv(a, 50000) = 0);

DROP TABLE report_metrics_v2;
