-- Tags: no-fasttest
-- no-fasttest: the S3 table engine is not available in the fast test build.

-- session_token, the Google ADC secrets (google_adc_client_secret, google_adc_refresh_token) and
-- the extra_credentials assume-role material (external_id) passed to the explicit-url or
-- named-collection S3 form must be masked like secret_access_key. Every secret value below is tagged
-- so the final assertion can prove none of them leaks. They used to leak in plaintext in SHOW CREATE
-- and logged query text.

-- Engine form: SHOW CREATE hides every secret, including all extra_credentials values.
DROP TABLE IF EXISTS t_04510;
CREATE TABLE t_04510 (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510', 'ak', 'SEKRIT_SAK',
            session_token = 'SEKRIT_ST',
            google_adc_client_secret = 'SEKRIT_ADCCS',
            google_adc_refresh_token = 'SEKRIT_ADCRT',
            extra_credentials(role_arn = 'SEKRIT_RA', external_id = 'SEKRIT_EID'),
            format = 'TSV');
SHOW CREATE TABLE t_04510 SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510;

-- Engine form with a positional session_token (4th positional argument) must be hidden too.
DROP TABLE IF EXISTS t_04510_pos;
CREATE TABLE t_04510_pos (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510pos', 'ak', 'SEKRIT_SAK', 'SEKRIT_POSTOK', 'TSV');
SHOW CREATE TABLE t_04510_pos SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510_pos;

-- The forms below all fail at analysis (empty host / missing collection) before any network access,
-- and are logged with secrets replaced.

-- Explicit-url function form.
SELECT * FROM s3('url_basic', 'ak', 'SEKRIT_SAK',
                 session_token = 'SEKRIT_ST',
                 google_adc_client_secret = 'SEKRIT_ADCCS',
                 google_adc_refresh_token = 'SEKRIT_ADCRT',
                 extra_credentials(external_id = 'SEKRIT_EID'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- extra_credentials placed between two named secret overrides must be masked as a nested map,
-- not swept into the named secret span (which would leak its first nested value).
SELECT * FROM s3('url_interleaved', secret_access_key = 'SEKRIT_SAK',
                 extra_credentials(external_id = 'SEKRIT_EID'),
                 session_token = 'SEKRIT_ST',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- Explicit-url function form with a positional session_token (4th positional argument).
SELECT * FROM s3('url_postoken', 'ak', 'SEKRIT_SAK', 'SEKRIT_POSTOK',
                 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- Named-collection form: an extra_credentials override alongside a collection must be masked too.
-- The collection need not exist; masking runs on the AST before the collection is resolved.
SELECT * FROM s3(nc_04510_missing, extra_credentials(external_id = 'SEKRIT_EID'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- BACKUP ... TO S3 explicit-url form.
BACKUP TABLE nonexistent_04510 TO S3('url_backup', 'ak', 'SEKRIT_SAK',
                 session_token = 'SEKRIT_ST',
                 google_adc_client_secret = 'SEKRIT_ADCCS',
                 google_adc_refresh_token = 'SEKRIT_ADCRT',
                 extra_credentials(external_id = 'SEKRIT_EID')); -- { serverError BAD_ARGUMENTS }

-- Backup database engine reconstructs the nested S3 destination; extra_credentials must be masked.
CREATE DATABASE db_04510_ec ENGINE = Backup('', S3('url_dbec', 'ak', 'SEKRIT_SAK',
                 extra_credentials(external_id = 'SEKRIT_EID'))); -- { serverError BAD_ARGUMENTS }

-- The reconstructor must fail closed on an unsupported tail (headers), not emit it verbatim.
CREATE DATABASE db_04510_hdr ENGINE = Backup('', S3('url_dbhdr', 'ak', 'SEKRIT_SAK',
                 headers('X-Auth' = 'SEKRIT_HDR'))); -- { serverError BAD_ARGUMENTS }

-- The reconstructor must also fail closed on a constant-expression extra_credentials key.
CREATE DATABASE db_04510_expr ENGINE = Backup('', S3('url_dbexpr', 'ak', 'SEKRIT_SAK',
                 extra_credentials(concat('extern', 'al_id') = 'SEKRIT_EXPR'))); -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;
-- Assert every form was logged and masked, and that no form leaks any secret (SEKRIT marker).
SELECT
    countIf(query LIKE '%url_basic%'        AND query LIKE '%[HIDDEN]%') > 0 AS s3_masked,
    countIf(query LIKE '%url_interleaved%'  AND query LIKE '%[HIDDEN]%') > 0 AS s3_interleaved_masked,
    countIf(query LIKE '%url_postoken%'     AND query LIKE '%[HIDDEN]%') > 0 AS s3_positional_session_token_masked,
    countIf(query LIKE '%nc_04510_missing%' AND query LIKE '%[HIDDEN]%') > 0 AS s3_named_collection_masked,
    countIf(query LIKE '%url_backup%'       AND query LIKE '%[HIDDEN]%') > 0 AS backup_masked,
    countIf(query LIKE '%db_04510_ec%'      AND query LIKE '%[HIDDEN]%') > 0 AS backup_db_masked,
    countIf(query LIKE '%db_04510_hdr%'     AND query LIKE '%[HIDDEN]%') > 0 AS backup_db_headers_masked,
    countIf(query LIKE '%db_04510_expr%'    AND query LIKE '%[HIDDEN]%') > 0 AS backup_db_expr_key_masked,
    countIf(query LIKE '%SEKRIT%') AS leaked
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query NOT LIKE '%query_log%' -- exclude this counting query itself
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE;
