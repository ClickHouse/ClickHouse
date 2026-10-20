SET send_logs_level = 'fatal';

SELECT format('{{{}}}', 'Hello');
SELECT format('{{{{}}}}', 'Hello');
SELECT format('{{{0}}}', materialize('Hello'));
SELECT format('{{{{{}}}}}', 'Hello');
SELECT format('{{{{{0}}}}}', materialize('Hello'));
SELECT format('{{}}{}', 'Hello');
SELECT format('{}{{}}', 'Hello');
SELECT format('{}}}', 'Hello');
