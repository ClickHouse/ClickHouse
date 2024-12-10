SELECT 'qfilelog';
CREATE TABLE qfilelog (key UInt64, value UInt64) ENGINE = FileLog('/tmp/app.log', 'JSONEachRow')
SETTINGS handle_error_mode = 'dead_letter_queue'; -- { serverError BAD_ARGUMENTS }

SELECT 'qnats';
CREATE TABLE qnats (key UInt64, value UInt64 ) ENGINE = NATS
SETTINGS nats_url = 'localhost:4444',
         nats_subjects = 'subject1,subject2',
				 nats_format = 'JSONEachRow',
				 nats_handle_error_mode = 'dead_letter_queue'; -- { serverError BAD_ARGUMENTS }
