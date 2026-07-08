-- Tags: no-fasttest
-- no-fasttest: the S3 table engine is not available in the fast test build.

-- session_token and the Google ADC secrets (google_adc_client_secret, google_adc_refresh_token)
-- passed as named arguments to the explicit-url S3 form must be masked like secret_access_key.
-- They used to leak in plaintext into SHOW CREATE and the logged query text.

DROP TABLE IF EXISTS t_04510;
CREATE TABLE t_04510 (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510', 'ak', 'sk',
            session_token = 'SESSIONTOKENSECRET',
            google_adc_client_secret = 'ADCCLIENTSECRET',
            google_adc_refresh_token = 'ADCREFRESHTOKEN', format = 'TSV');
SHOW CREATE TABLE t_04510 SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510;
