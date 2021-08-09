SET send_logs_level = 'fatal';
SET join_default_strictness = '';
SELECT * FROM system.one INNER JOIN (SELECT number AS k FROM system.numbers) js2 ON dummy = k; -- { serverError 417 }
