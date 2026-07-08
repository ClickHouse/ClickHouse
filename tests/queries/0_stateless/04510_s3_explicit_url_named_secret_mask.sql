-- Tags: no-fasttest
-- no-fasttest: the S3 table engine is not available in the fast test build.

-- session_token and the Google ADC secrets (google_adc_client_secret, google_adc_refresh_token)
-- passed as named arguments to the explicit-url S3 form must be masked like secret_access_key.
-- They used to leak in plaintext into SHOW CREATE and the logged query text.

-- Engine form: masked in SHOW CREATE.
DROP TABLE IF EXISTS t_04510;
CREATE TABLE t_04510 (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510', 'ak', 'sk',
            session_token = 'SESSIONTOKENSECRET',
            google_adc_client_secret = 'ADCCLIENTSECRET',
            google_adc_refresh_token = 'ADCREFRESHTOKEN', format = 'TSV');
SHOW CREATE TABLE t_04510 SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510;

-- Function and BACKUP forms: masked in the logged query text. Both fail at analysis (empty
-- host / unknown table) before any network access, and are logged with secrets replaced.
SELECT * FROM s3('url', 'ak', 'sk',
                 session_token = 'SESSIONTOKENSECRET',
                 google_adc_client_secret = 'ADCCLIENTSECRET',
                 google_adc_refresh_token = 'ADCREFRESHTOKEN',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

BACKUP TABLE nonexistent_04510 TO S3('url', 'ak', 'sk',
                 session_token = 'SESSIONTOKENSECRET',
                 google_adc_client_secret = 'ADCCLIENTSECRET',
                 google_adc_refresh_token = 'ADCREFRESHTOKEN'); -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;
SELECT
    countIf(query LIKE '%SESSIONTOKENSECRET%'
         OR query LIKE '%ADCCLIENTSECRET%'
         OR query LIKE '%ADCREFRESHTOKEN%') AS leaked,
    countIf(query LIKE '%[HIDDEN]%') > 0 AS masked
FROM system.query_log
WHERE current_database = currentDatabase()
  AND (query LIKE 'SELECT % FROM s3(%' OR query LIKE 'BACKUP %')
  AND query NOT LIKE '%query_log%' -- exclude this counting query itself
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE;
