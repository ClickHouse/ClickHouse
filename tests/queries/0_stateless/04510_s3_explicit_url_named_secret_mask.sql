-- Tags: no-fasttest
-- no-fasttest: the S3 table engine is not available in the fast test build.

-- session_token, the Google ADC secrets (google_adc_client_secret, google_adc_refresh_token) and
-- the extra_credentials assume-role material (external_id) passed to the explicit-url S3 form must
-- be masked like secret_access_key. They used to leak in plaintext in SHOW CREATE and logged queries.

-- Engine form: masked in SHOW CREATE.
DROP TABLE IF EXISTS t_04510;
CREATE TABLE t_04510 (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510', 'ak', 'sk',
            session_token = 'SESSIONTOKENSECRET',
            google_adc_client_secret = 'ADCCLIENTSECRET',
            google_adc_refresh_token = 'ADCREFRESHTOKEN',
            extra_credentials(role_arn = 'MYROLEARN', external_id = 'EXTERNALIDSECRET'),
            format = 'TSV');
SHOW CREATE TABLE t_04510 SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510;

-- Function and BACKUP forms: masked in the logged query text. Both fail at analysis (empty host)
-- before any network access, and are logged with secrets replaced.
SELECT * FROM s3('url', 'ak', 'sk',
                 session_token = 'SESSIONTOKENSECRET',
                 google_adc_client_secret = 'ADCCLIENTSECRET',
                 google_adc_refresh_token = 'ADCREFRESHTOKEN',
                 extra_credentials(external_id = 'EXTERNALIDSECRET'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

BACKUP TABLE nonexistent_04510 TO S3('url', 'ak', 'sk',
                 session_token = 'SESSIONTOKENSECRET',
                 google_adc_client_secret = 'ADCCLIENTSECRET',
                 google_adc_refresh_token = 'ADCREFRESHTOKEN',
                 extra_credentials(external_id = 'EXTERNALIDSECRET')); -- { serverError BAD_ARGUMENTS }

-- Backup database engine reconstructs the nested S3 destination; extra_credentials must be masked.
CREATE DATABASE db_04510 ENGINE = Backup('', S3('url', 'ak', 'sk',
                 extra_credentials(external_id = 'EXTERNALIDSECRET'))); -- { serverError BAD_ARGUMENTS }

-- Named-collection form: an extra_credentials override alongside a collection must be masked too.
-- The collection need not exist; masking runs on the AST before the collection is resolved.
SELECT * FROM s3(nc_04510_missing, extra_credentials(external_id = 'EXTERNALIDSECRET'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

SYSTEM FLUSH LOGS query_log;
-- Assert each explicit-url and named-collection form is individually logged and masked,
-- and that no form leaks any secret.
SELECT
    countIf(query LIKE '% FROM s3(''url''%' AND query LIKE '%[HIDDEN]%') > 0 AS s3_masked,
    countIf(query LIKE '%s3(nc_04510_missing%' AND query LIKE '%[HIDDEN]%') > 0 AS s3_named_collection_masked,
    countIf(query LIKE 'BACKUP %' AND query LIKE '%[HIDDEN]%') > 0 AS backup_masked,
    countIf(query LIKE 'CREATE DATABASE%Backup%' AND query LIKE '%[HIDDEN]%') > 0 AS backup_db_masked,
    countIf((query LIKE 'SELECT % FROM s3(%' OR query LIKE 'BACKUP %' OR query LIKE 'CREATE DATABASE%Backup%')
         AND (query LIKE '%SESSIONTOKENSECRET%' OR query LIKE '%ADCCLIENTSECRET%'
           OR query LIKE '%ADCREFRESHTOKEN%' OR query LIKE '%EXTERNALIDSECRET%')) AS leaked
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query NOT LIKE '%query_log%' -- exclude this counting query itself
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE;
