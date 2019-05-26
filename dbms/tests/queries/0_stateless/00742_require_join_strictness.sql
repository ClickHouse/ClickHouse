SET send_logs_level = 'none';
SET join_default_strictness = '';
SELECT * FROM system.one INNER JOIN (SELECT number AS k FROM system.numbers) ON dummy = k; -- { serverError 417 }
