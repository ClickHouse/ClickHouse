-- Different values for tests with DatabaseReplicated and without.
SELECT value == '127.0.0.1:9181' OR value == 'localhost:9181,localhost:19181,localhost:29181'
FROM system.server_settings WHERE name = 'keeper_hosts';
