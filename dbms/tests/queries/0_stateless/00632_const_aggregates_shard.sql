SELECT toDate('2000-01-01', any('UTC'));
SELECT toDate('2000-01-01', any('UTC')), count() AS c FROM (SELECT * FROM system.numbers LIMIT 3) GROUP BY number % 2 ORDER BY c;
SELECT toDate('2000-01-01', any('UTC')), any('UTC'), count() AS c FROM system.one WHERE 0;
SELECT any('UTC'), count() AS c FROM remote('127.0.0.1,127.0.0.2', 'system', 'one');