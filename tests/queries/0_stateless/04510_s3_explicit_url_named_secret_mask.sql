-- Tags: no-fasttest
-- no-fasttest: the S3 table engine is not available in the fast test build.

-- session_token, the Google ADC secrets (google_adc_client_secret, google_adc_refresh_token) and
-- the extra_credentials assume-role material (external_id) passed to the explicit-url or
-- named-collection S3 form must be masked like secret_access_key. Every secret value below is tagged
-- so the final assertion can prove none of them leaks. They used to leak in plaintext in SHOW CREATE
-- and logged query text.

-- Engine form: SHOW CREATE hides every secret; the non-secret extra_credentials identifiers
-- (role_arn, role_session_name) stay visible while external_id is hidden.
DROP TABLE IF EXISTS t_04510;
CREATE TABLE t_04510 (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510', 'ak', 'SEKRIT_SAK',
            session_token = 'SEKRIT_ST',
            google_adc_client_secret = 'SEKRIT_ADCCS',
            google_adc_refresh_token = 'SEKRIT_ADCRT',
            extra_credentials(role_arn = 'visible_role_arn', external_id = 'SEKRIT_EID'),
            format = 'TSV');
SHOW CREATE TABLE t_04510 SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510;

-- Engine form with a positional session_token (4th positional argument) must be hidden too.
DROP TABLE IF EXISTS t_04510_pos;
CREATE TABLE t_04510_pos (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510pos', 'ak', 'SEKRIT_SAK', 'SEKRIT_POSTOK', 'TSV');
SHOW CREATE TABLE t_04510_pos SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510_pos;

-- The parser strips nested maps from any position before assigning positional slots, so a map
-- placed before the positional session_token must not shift the token out of the masked slot.
DROP TABLE IF EXISTS t_04510_mid;
CREATE TABLE t_04510_mid (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510mid', 'ak', 'SEKRIT_SAK',
            headers('Authorization' = 'SEKRIT_HDR'), 'SEKRIT_MIDTOK', 'TSV');
SHOW CREATE TABLE t_04510_mid SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510_mid;

-- A constant-expression format at the session_token slot is valid (the parser evaluates it). The
-- storage stores the evaluated literal, so SHOW CREATE keeps the format visible. In the logged text
-- of the original query the unevaluated expression is indistinguishable from a session token (which
-- would show its pieces verbatim), so there it is hidden: fail closed.
DROP TABLE IF EXISTS t_04510_exprfmt;
CREATE TABLE t_04510_exprfmt (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510exprfmt', 'ak', 'SEKRIT_SAK', concat('TS', 'V'), 'none');
SHOW CREATE TABLE t_04510_exprfmt SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510_exprfmt;

-- Five positional arguments in the engine form (no positional structure) is the access-key signature,
-- not a NOSIGN one: NOSIGN sits in the access_key_id slot and the following argument is the
-- secret_access_key, which must be hidden even though the leading token reads as NOSIGN.
DROP TABLE IF EXISTS t_04510_nosign5;
CREATE TABLE t_04510_nosign5 (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510nosign5', NOSIGN, 'SEKRIT_NOSIGNSAK', 'CSV', 'none');
SHOW CREATE TABLE t_04510_nosign5 SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510_nosign5;

-- Six positional arguments in the engine form fix the session_token at the 4th slot regardless of its
-- value, so a session_token that happens to spell a registered format name must still be hidden.
DROP TABLE IF EXISTS t_04510_parqtok;
CREATE TABLE t_04510_parqtok (x UInt8)
ENGINE = S3('http://localhost:11111/test/04510parqtok', 'ak', 'SEKRIT_PARQSAK', 'Parquet', 'CSV', 'none');
SHOW CREATE TABLE t_04510_parqtok SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510_parqtok;

-- Every S3-backed table engine shares the S3 credential signature, so SHOW CREATE must hide the
-- secret_access_key for the whole family (GCS, the data-lake engines, ...), not only for `S3`.
DROP TABLE IF EXISTS t_04510_gcs;
CREATE TABLE t_04510_gcs (x UInt8)
ENGINE = GCS('http://localhost:11111/test/04510gcs', 'ak', 'SEKRIT_GCSSAK', 'TSV');
SHOW CREATE TABLE t_04510_gcs SETTINGS format_display_secrets_in_show_and_select = 0;
DROP TABLE t_04510_gcs;

-- The forms below all fail at analysis (empty host / missing collection) before any network access,
-- and are logged with secrets replaced. Each carries a unique marker checked by the final assertion.

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

-- Secrets can be non-contiguous in valid syntax; the non-secret arguments in between (format,
-- structure) must stay visible.
SELECT * FROM s3('url_noncontig', 'ak', 'SEKRIT_SAK', 'TSV', 'x UInt8',
                 session_token = 'SEKRIT_TAILTOK'); -- { serverError BAD_ARGUMENTS }

-- The five-positional NOSIGN form carries no credentials; nothing must be masked.
SELECT * FROM s3('url_nosign5', NOSIGN, 'TSV', 'x UInt8', 'none'); -- { serverError BAD_ARGUMENTS }

-- A nested map before the positional session_token must not shift it out of the masked slot.
SELECT * FROM s3('url_midtok', 'ak', 'SEKRIT_SAK',
                 extra_credentials(external_id = 'SEKRIT_EID'),
                 'SEKRIT_MIDTOK', 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- A duplicated secret key is malformed but formatted for logging before validation rejects it, so
-- every occurrence must be masked, not just the first.
SELECT * FROM s3('url_dup', 'ak', 'sk',
                 session_token = 'SEKRIT_DUP1',
                 format = 'TSV',
                 session_token = 'SEKRIT_DUP2',
                 structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- A nested map with a malformed child (not `key = value`) must fail closed in formatting.
-- The analyzer rejects `extra_credentials` as an unknown function; with `enable_analyzer = 0`
-- the S3 URI validation is reached first.
SELECT * FROM s3('url_badmap', 'ak', 'SEKRIT_SAK',
                 extra_credentials('SEKRIT_RAWCRED'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError UNKNOWN_FUNCTION, BAD_ARGUMENTS }

-- The parser rejects a positional after the first key = value argument, but the query is logged
-- first and the intended slot is unknowable, so the positional must be masked.
SELECT * FROM s3('url_posafter', access_key_id = 'ak', 'SEKRIT_SK',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- The parser evaluates constant-expression keys, so this can be an effective session_token override;
-- the value must be masked without evaluating the key.
SELECT * FROM s3('url_exprkey', 'ak', 'SEKRIT_SAK',
                 concat('session_', 'token') = 'SEKRIT_EXPRTOK',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- A nested map placed as the value of a visible non-secret key must be hidden, not formatted verbatim:
-- the value is not a plain literal, so it fails closed before the parser rejects the non-literal value.
SELECT * FROM s3('url_fmthdr', format = headers('Authorization' = 'SEKRIT_FMTHDR'),
                 structure = 'x UInt8'); -- { serverError BAD_ARGUMENTS, UNKNOWN_FUNCTION }

-- Same inside extra_credentials: role_arn stays visible only for a literal/identifier value; a nested
-- map value carries a secret and must be hidden.
SELECT * FROM s3('url_rolehdr', 'ak', 'SEKRIT_SAK',
                 extra_credentials(role_arn = headers('Authorization' = 'SEKRIT_ROLEHDR'))); -- { serverError BAD_ARGUMENTS, UNKNOWN_FUNCTION }

-- A `structure = ...` override given as a constant expression is evaluated by the parser, which then
-- drops the positional structure slot: the leading positional is then a secret_access_key, not a
-- format, and must be hidden even though it reads as a format name. The masker cannot evaluate the
-- key, so it fails closed and drops the positional structure slot for any unevaluable key.
SELECT * FROM s3('url_exprstruct', 'CSV', 'SEKRIT_EXPRSTRUCT', 'TSV',
                 concat('struc', 'ture') = 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- The URL itself can carry credentials: the userinfo and presigned-URL query parameters must be
-- masked while the host, path and non-credential parameters stay visible. The one-character bucket
-- makes S3 URI validation reject the query before any network access.
SELECT * FROM s3('https://user:SEKRIT_PW@localhost:11111/x?X-Amz-Signature=SEKRIT_SIG&partNumber=7',
                 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- A password may itself contain '@'; the userinfo must be masked up to the last '@' before the host,
-- not just the first, so no fragment of it stays visible.
SELECT * FROM s3('https://user:SEKRIT_PWA@SEKRIT_PWB@localhost:11111/x?X-Amz-Signature=SEKRIT_SIG',
                 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- A retained URL part can legally contain an apostrophe; the partially-masked url must be re-emitted
-- as a properly escaped SQL literal (not by naive quoting, which would produce a broken statement).
SELECT * FROM s3('https://user:SEKRIT_PW@localhost:11111/x/o''clock?X-Amz-Signature=SEKRIT_SIG',
                 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- A url built from a constant expression is evaluated by the parser and can embed credentials in
-- its pieces; the masker cannot evaluate it, so the whole url argument is hidden (fail closed).
SELECT * FROM s3(concat('https://user:SEKRIT_PW@localhost:11111/x?X-Amz-Signature=', 'SEKRIT_SIG'),
                 'TSV', 'x UInt8'); -- { serverError BAD_ARGUMENTS }

-- Same for BACKUP and the Backup database reconstructor.
BACKUP TABLE nonexistent_04510 TO S3('https://user:SEKRIT_PW@localhost:11111/x?X-Amz-Signature=SEKRIT_SIG',
                 'ak', 'SEKRIT_SAK'); -- { serverError BAD_ARGUMENTS }
CREATE DATABASE db_04510_authurl ENGINE = Backup('', S3('https://user:SEKRIT_PW@localhost:11111/x?X-Amz-Signature=SEKRIT_SIG',
                 'ak', 'SEKRIT_SAK')); -- { serverError BAD_ARGUMENTS }

-- Named-collection form: an extra_credentials override alongside a collection must be masked too.
-- The collection need not exist; masking runs on the AST before the collection is resolved.
SELECT * FROM s3(nc_04510_missing, extra_credentials(external_id = 'SEKRIT_EID'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- A url override on a named collection can carry credentials too.
SELECT * FROM s3(nc_authurl_missing, url = 'https://user:SEKRIT_PW@localhost:11111/x?X-Amz-Signature=SEKRIT_SIG',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- Named-collection form: a headers() override must have its values masked too.
SELECT * FROM s3(nc_headers_missing, headers('Authorization' = 'SEKRIT_HDRVAL'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- Named-collection form: a headers() override with a malformed child must fail closed.
SELECT * FROM s3(nc_badhdr_missing, headers('Authorization: SEKRIT_RAWHDR'),
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- A positional argument swept inside a named secret span must not be echoed as a bogus key.
SELECT * FROM s3(nc_span_missing, secret_access_key = 'SEKRIT_SPAN1', 'SEKRIT_MIDPOS',
                 session_token = 'SEKRIT_SPAN2',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- A constant-expression key can be an effective secret override for a named collection too.
SELECT * FROM s3(nc_exprkey_missing, concat('secret_', 'access_key') = 'SEKRIT_EXPRVAL',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- The named-collection form permits no positional argument at all, so one placed before the first
-- named override must also be masked.
SELECT * FROM s3(nc_prepos_missing, 'SEKRIT_PREPOS',
                 secret_access_key = 'SEKRIT_SK',
                 format = 'TSV', structure = 'x UInt8'); -- { serverError NAMED_COLLECTION_DOESNT_EXIST }

-- BACKUP ... TO S3 explicit-url form.
BACKUP TABLE nonexistent_04510 TO S3('url_bkp_named', 'ak', 'SEKRIT_SAK',
                 session_token = 'SEKRIT_ST',
                 google_adc_client_secret = 'SEKRIT_ADCCS',
                 google_adc_refresh_token = 'SEKRIT_ADCRT',
                 extra_credentials(external_id = 'SEKRIT_EID')); -- { serverError BAD_ARGUMENTS }

-- BACKUP ... TO S3 with an invalid 4th positional argument (a session token) is rejected by the
-- backup engine, but the positional token must still be masked in the logged query text.
BACKUP TABLE nonexistent_04510 TO S3('url_bkp_pos', 'ak', 'SEKRIT_SAK',
                 'SEKRIT_BACKUPTOK'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- The backup named-collection locator accepts one positional: the non-secret filename, which must
-- stay visible; any positional beyond it is invalid and must be masked.
BACKUP TABLE nonexistent_04510 TO S3(nc_bkp_missing, 'visible_bkp_dir',
                 'SEKRIT_BKPNCPOS'); -- { serverError BAD_ARGUMENTS }

-- The filename is collected independently of named overrides, so it stays visible after one too.
BACKUP TABLE nonexistent_04510 TO S3(nc_bkporder_missing,
                 secret_access_key = 'SEKRIT_BKPORD', 'visible_bkp_dir2'); -- { serverError BAD_ARGUMENTS }

-- An explicit-url locator with an invalid positional count (neither 1 nor 3): the intended slots
-- are unknowable, so everything after the url must be masked.
BACKUP TABLE nonexistent_04510 TO S3('url_bkp_mixed',
                 access_key_id = 'ak', 'SEKRIT_BKPMIX'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Backup database engine reconstructs the nested S3 destination; extra_credentials must be masked.
CREATE DATABASE db_04510_ec ENGINE = Backup('', S3('url_dbec', 'ak', 'SEKRIT_SAK',
                 extra_credentials(external_id = 'SEKRIT_EID'))); -- { serverError BAD_ARGUMENTS }

-- The reconstructor must fail closed on an invalid extra positional argument (a session token).
CREATE DATABASE db_04510_postok ENGINE = Backup('', S3('url_dbpostok', 'ak', 'SEKRIT_SAK',
                 'SEKRIT_DBTOK')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- Named-collection locator in the reconstructor: the filename stays visible, but a second positional
-- is invalid and must be masked.
CREATE DATABASE db_04510_ncpos ENGINE = Backup('', S3(nc_dbnc_missing, 'visible_dbnc_dir',
                 'SEKRIT_DBNCPOS')); -- { serverError BAD_ARGUMENTS }

-- The reconstructor also keeps the filename visible when it follows a named override.
CREATE DATABASE db_04510_ncorder ENGINE = Backup('', S3(nc_dbord_missing,
                 secret_access_key = 'SEKRIT_DBORD', 'visible_dbnc_dir2')); -- { serverError BAD_ARGUMENTS }

-- Non-string scalar overrides are valid and non-secret; the reconstructor keeps them visible.
CREATE DATABASE db_04510_ncenv ENGINE = Backup('', S3(nc_dbenv_missing,
                 secret_access_key = 'SEKRIT_DBENVKEY', use_environment_credentials = 1)); -- { serverError BAD_ARGUMENTS }

-- A non-secret named value that is an expression (a computed filename, or a nested headers() /
-- extra_credentials() map) is accepted by the backup named-collection path, which the parser evaluates
-- as a constant. The reconstructor cannot classify it and its formatted text would carry any nested
-- secret verbatim, so it fails closed to [HIDDEN] rather than leak.
CREATE DATABASE db_04510_ncexpr ENGINE = Backup('', S3(nc_dbexpr_missing,
                 secret_access_key = 'SEKRIT_DBEXPRKEY',
                 filename = headers('Authorization' = 'SEKRIT_NESTEDHDR'))); -- { serverError BAD_ARGUMENTS }

-- The reconstructor masks everything after the url on an invalid positional count too.
CREATE DATABASE db_04510_mixed ENGINE = Backup('', S3('url_dbmixed',
                 access_key_id = 'ak', 'SEKRIT_DBMIX')); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A url override built from an expression can embed credentials in its pieces; the reconstructor
-- must hide it even when it is the only secret-bearing argument.
CREATE DATABASE db_04510_ncurl ENGINE = Backup('', S3(nc_dburl_missing,
                 url = concat('https://user:SEKRIT_PW@', 'localhost/x?X-Amz-Signature=SEKRIT_SIG'))); -- { serverError BAD_ARGUMENTS }

-- The reconstructor must fail closed on an unsupported tail (headers), not emit it verbatim.
CREATE DATABASE db_04510_hdr ENGINE = Backup('', S3('url_dbhdr', 'ak', 'SEKRIT_SAK',
                 headers('X-Auth' = 'SEKRIT_HDR'))); -- { serverError BAD_ARGUMENTS }

-- The reconstructor must also fail closed on a constant-expression extra_credentials key.
CREATE DATABASE db_04510_expr ENGINE = Backup('', S3('url_dbexpr', 'ak', 'SEKRIT_SAK',
                 extra_credentials(concat('extern', 'al_id') = 'SEKRIT_EXPR'))); -- { serverError BAD_ARGUMENTS }

-- The S3 database engine accepts no positional beyond secret_access_key; an extra positional must
-- be masked in the logged query text.
CREATE DATABASE db_04510_s3pos ENGINE = S3('url_dbs3pos', 'ak', 'SEKRIT_SAK',
                 'SEKRIT_S3DBTOK'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A valid non-secret named override (use_environment_credentials) must stay visible while
-- secret_access_key is hidden. This CREATE succeeds (the S3 database is lazy), so use a unique
-- database name to avoid collisions across parallel runs, and drop it after.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = S3('url_dbenv', 'ak', 'SEKRIT_SAK', use_environment_credentials = 1);
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

-- The query-tree surface (EXPLAIN QUERY TREE) must hide the same carriers as the logged query text:
-- a credential-bearing url (masked whole, since a tree dump cannot represent partial masking), the
-- positional secrets, and the values of headers(...) / extra_credentials(...).
-- run_passes = 0 keeps the table function unresolved: the masking visitor runs before the passes
-- either way, and resolution would touch the storage (URI validation, credential checks), which
-- varies across test configurations.
SET enable_analyzer = 1;
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3('https://user:SEKRIT_PW@localhost:11111/test/04510qt?X-Amz-Signature=SEKRIT_SIG', 'ak', 'SEKRIT_SAK', 'TSV', 'x UInt8');
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3('http://localhost:11111/test/04510qt', NOSIGN, 'TSV', 'x UInt8', headers('Authorization' = 'SEKRIT_HDR'));
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3('http://localhost:11111/test/04510qt', 'ak', 'SEKRIT_SAK', 'TSV', 'x UInt8', extra_credentials(external_id = 'SEKRIT_EID'));

-- Identifier-valued secrets (the parsers evaluate identifiers as literals) have no display mask of
-- their own, so the dump-only tree replaces them with a hidden constant.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3('http://localhost:11111/test/04510qt', 'ak', 'SEKRIT_SAK', 'TSV', 'x UInt8', session_token = SEKRIT_IDTOK);
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3('http://localhost:11111/test/04510qt', NOSIGN, 'TSV', 'x UInt8', headers('Authorization' = SEKRIT_BEARER));

-- A named url override is an `equals` node in the tree; its credential-bearing value must be hidden.
-- Without passes the collection need not exist.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3(nc_04510_missing, url = 'https://user:SEKRIT_PW@localhost:11111/test/04510qt?X-Amz-Signature=SEKRIT_SIG', structure = 'x UInt8');

-- A table function can sit under a UNION (or any other carrier); the dump masking visitor must descend
-- into every node, not only query/join carriers, or the secret leaks in the tree dump.
EXPLAIN QUERY TREE run_passes = 0 SELECT * FROM s3('http://localhost:11111/test/04510qt', 'ak', 'SEKRIT_UNIONSAK', 'TSV', 'x UInt8') UNION ALL SELECT 1;

SYSTEM FLUSH LOGS query_log;

-- The exact logged text of every query above, in execution order: secrets must appear as '[HIDDEN]'
-- while every non-secret part (urls, formats, structures, filenames, non-secret overrides) stays
-- visible verbatim. Each query has exactly one terminal event: QueryFinish for the successful ones,
-- an exception event for the rejected ones.
SELECT query
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND query_kind != 'Set' -- sent by the test harness, not by this test
  AND query NOT ILIKE 'SYSTEM FLUSH%' -- its own terminal event races with the flush it performs
  AND query_id = initial_query_id -- only the statements issued here: a Replicated database logs
                                  -- each DDL again from the replay worker, which inherits the
                                  -- initiator's initial_query_id but gets a fresh query_id
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE
ORDER BY event_time_microseconds;

-- The transcript above is scoped to the statements this test issued. A Replicated database also
-- logs each DDL from the replay worker, which re-masks a rewritten AST independently, so assert
-- the masking property over every row this test produced, replay rows included. count() > 0 keeps
-- an empty row set from passing vacuously.
SELECT count() > 0, countIf(query LIKE '%SEKRIT%')
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type != 'QueryStart'
  AND event_date >= yesterday() AND event_time > now() - INTERVAL 5 MINUTE;
