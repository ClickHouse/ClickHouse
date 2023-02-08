-- Tags: no-fasttest
SELECT *
FROM mysql('127.0.0.1:9004', system, one, 'default', '')
SETTINGS send_logs_level = 'fatal'; -- failed connection tries are ok, if it succeeded after retry.
