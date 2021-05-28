SELECT x, y, arrayJoin(['a', NULL, 'b']) AS z FROM system.one ARRAY JOIN [1, NULL, 3] AS x, [(NULL, ''), (123, 'Hello'), (456, NULL)] AS y;
