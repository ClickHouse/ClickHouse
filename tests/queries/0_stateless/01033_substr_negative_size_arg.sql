SELECT substr('clickhouse', 2, -2);
SELECT substr(materialize('clickhouse'), 2, -2);
SELECT substr('clickhouse', materialize(2), -2);
SELECT substr(materialize('clickhouse'), materialize(2), -2);
SELECT substr('clickhouse', 2, materialize(-2));
SELECT substr(materialize('clickhouse'), 2, materialize(-2));
SELECT substr('clickhouse', materialize(2), materialize(-2));
SELECT substr(materialize('clickhouse'), materialize(2), materialize(-2));
