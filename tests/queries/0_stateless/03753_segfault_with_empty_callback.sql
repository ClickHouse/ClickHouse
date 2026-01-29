SYSTEM ENABLE FAILPOINT execute_query_calling_empty_set_result_func_on_exception;
SELECT 1 FROM url('http://localhost:8123/?query=SELECT+1+FROM+t0+FORMAT+JSON', 'JSON', 'c0 Int') tx; -- { serverError RECEIVED_ERROR_FROM_REMOTE_IO_SERVER }
