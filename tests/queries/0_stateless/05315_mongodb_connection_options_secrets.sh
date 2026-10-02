#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the MongoDB table engine, table function and dictionary source need USE_MONGODB
# no-replicated-database: a named collection is server-global, not database-scoped

# The values of the MongoDB connection options that carry a secret (tlsCertificateKeyFilePassword,
# sslClientCertificateKeyPassword, AWS_SESSION_TOKEN in authMechanismProperties) must be hidden wherever a
# MongoDB connection string or option list is shown: SHOW CREATE (system.tables), system.dictionaries.source
# and system.query_log. Every secret below contains the marker OPTSECRET. None of these objects contacts a
# MongoDB server when it is created or loaded.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

NC="nc_05315_${CLICKHOUSE_DATABASE}"
NC2="nc2_05315_${CLICKHOUSE_DATABASE}"
PROBE="05315_probe_${CLICKHOUSE_DATABASE}"

# A statement carrying a secret. A rejected one prints the name of its object and the error code.
# Further arguments are passed to the client.
probe()
{
    $CLICKHOUSE_CLIENT --log_queries=1 --log_comment "$PROBE" "${@:2}" -q "$1" 2>&1 | grep -oE '^Code: [0-9]+' | sed "s/^/$(cut -d' ' -f3 <<< "$1") /"
}

$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC}"
$CLICKHOUSE_CLIENT -q "DROP NAMED COLLECTION IF EXISTS ${NC2}"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC} AS host = '127.0.0.1', port = 27017, user = '', password = '', database = 'db', collection = 'c'"
$CLICKHOUSE_CLIENT -q "CREATE NAMED COLLECTION ${NC2} AS collection = 'c'"

# Table engine: the URI forms, the positional options of the host:port form, the named-collection overrides
# (a uri after another override), a percent-encoded authMechanismProperties, and a positional URI after the
# collection name (rejected after being logged).
probe "CREATE TABLE e01 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db?tls=true&tlsCertificateKeyFilePassword=OPTSECRET1', 'c')"
probe "CREATE TABLE e02 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db?sslClientCertificateKeyPassword=OPTSECRET2', 'c', '_id')"
probe "CREATE TABLE e03 (x String) ENGINE = MongoDB(concat('mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=', 'OPTSECRET15'), 'c', '_id')"
probe "CREATE TABLE e04 (x String) ENGINE = MongoDB('127.0.0.1:27017', 'db', 'c', 'usr', 'pw', 'appName=keep&tlsCertificateKeyFilePassword=OPTSECRET3')"
probe "CREATE TABLE e05 (x String) ENGINE = MongoDB('127.0.0.1:27017', 'db', 'c', 'usr', 'pw', concat('tlsCertificateKeyFilePassword=', 'OPTSECRET4'))"
# ast_fuzzer_any_query = 0: a fuzzed DETACH of a table that uses a collection, or a `__fuzz_N` clone of it,
# would leave metadata naming a collection that is dropped at the end.
probe "CREATE TABLE e06 (x String) ENGINE = MongoDB(${NC}, options = 'tls=true&tlsCertificateKeyFilePassword=OPTSECRET19')" --ast_fuzzer_any_query=0
probe "CREATE TABLE e07 (x String) ENGINE = MongoDB(${NC}, concat('op', 'tions') = 'tlsCertificateKeyFilePassword=OPTSECRET13')" --ast_fuzzer_any_query=0
probe "CREATE TABLE e08 (x String) ENGINE = MongoDB(${NC2}, collection = 'c2', uri = 'mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=OPTSECRET25')" --ast_fuzzer_any_query=0
probe "CREATE TABLE e09 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db?authMechanismProperties=ENVIRONMENT:azure%2CAWS_SESSION_TOKEN%3AOPTSECRET26', 'c')"
probe "CREATE TABLE e10 (x String) ENGINE = MongoDB(${NC}, 'mongodb://usr:OPTSECRET27@127.0.0.1:27017/db?tlsCertificateKeyFilePassword=OPTSECRET27B')" --ast_fuzzer_any_query=0

# Table function: the URI form with a public property kept, the positional and the named options of the
# host:port form, a URI written after a named argument, an upper-case OPTIONS key (both rejected after being
# logged), a named oid_columns bound to the options slot, a named options and a positional after
# structure moved into the password slot, and a positional after a named collection (rejected after being logged).
probe "CREATE VIEW f01 AS SELECT * FROM mongodb('mongodb://127.0.0.1:27017/db?authMechanismProperties=SERVICE_NAME:keep,aws_session_token:OPTSECRET5,OPTSECRET5B', 'c', 'x String')"
probe "CREATE VIEW f02 AS SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'c', 'usr', 'pw', 'x String', 'tlsCertificateKeyFilePassword=OPTSECRET6')"
probe "CREATE VIEW f03 AS SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'c', 'usr', 'pw', 'x String', options = 'TLSCERTIFICATEKEYFILEPASSWORD=OPTSECRET7')"
probe "CREATE VIEW f04 AS SELECT * FROM mongodb(structure = 'x String', 'mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=OPTSECRET16', 'c')"
probe "CREATE VIEW f05 AS SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'c', 'usr', 'pw', 'x String', OPTIONS = 'tlsCertificateKeyFilePassword=OPTSECRET17')"
probe "CREATE VIEW f06 AS SELECT * FROM mongodb(oid_columns = 'tlsCertificateKeyFilePassword=OPTSECRET22', '127.0.0.1:27017', 'db', 'c', '', 'x String')"
probe "CREATE VIEW f07 AS SELECT * FROM mongodb(options = 'OPTSECRET23', '127.0.0.1:27017', 'db', 'c', 'usr', 'x String')"
probe "CREATE VIEW f08 AS SELECT * FROM mongodb(oid_columns = 'appName=keep', '127.0.0.1:27017', 'db', 'c', 'usr', 'x String', 'OPTSECRET24')"
probe "CREATE VIEW f09 AS SELECT * FROM mongodb(${NC}, structure = 'x String', 'OPTSECRET28')" --ast_fuzzer_any_query=0

# Dictionary source: an option given twice, a URI written as an identifier with '#' in the value, the
# OPTIONS of the host form, and OPTIONS given as an expression.
probe "CREATE DICTIONARY d01 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(URI 'mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=OPTSECRET8&tlsCertificateKeyFilePassword=OPTSECRET9' COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT())"
probe "CREATE DICTIONARY d02 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(URI \`mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=OPTSECRET10#OPTSECRET10B\` COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT())"
probe "CREATE DICTIONARY d03 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(HOST '127.0.0.1' PORT 27017 DB 'db' COLLECTION 'c' OPTIONS 'tls=true&tlsCertificateKeyFilePassword=OPTSECRET11')) LAYOUT(COMPLEX_KEY_DIRECT())"
probe "CREATE DICTIONARY d04 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(HOST '127.0.0.1' PORT 27017 DB 'db' COLLECTION 'c' OPTIONS concat('tlsCertificateKeyFilePassword=', 'OPTSECRET12'))) LAYOUT(COMPLEX_KEY_DIRECT())"

# With a user and password: an '@' inside an option value (rejected after being logged), a password that
# contains '?', '=' and '&' around option text, and the password of the host form in the dictionary source.
probe "CREATE TABLE u01 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db?tlsCertificateKeyFilePassword=OPTSECRET14@OPTSECRET14B', 'c')"
probe "CREATE TABLE u02 (x String) ENGINE = MongoDB('mongodb://usr:OPTSECRET18A?tlsCertificateKeyFilePassword=OPTSECRET18B@127.0.0.1:27017/db', 'c')"
probe "CREATE DICTIONARY u03 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(URI 'mongodb://usr:OPTSECRET18C?tlsCertificateKeyFilePassword=OPTSECRET18D@127.0.0.1:27017/db' COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT())"
probe "CREATE TABLE u04 (x String) ENGINE = MongoDB('mongodb://u?tlsCertificateKeyFilePassword=OPTSECRET20A:OPTSECRET20B&OPTSECRET20C@127.0.0.1:27017/db', 'c')"
probe "CREATE DICTIONARY u05 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(HOST '127.0.0.1' PORT 27017 USER 'usr' PASSWORD 'OPTSECRET21' DB 'db' COLLECTION 'c')) LAYOUT(COMPLEX_KEY_DIRECT())"

$CLICKHOUSE_CLIENT -m -q "
-- Controls, shown as written: options without a secret, a computed oid_columns, a collection name, and a
-- named oid_columns after five positionals.
CREATE TABLE c01 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db?tls=true&appName=keep', 'c');
CREATE DICTIONARY c02 (_id String, v String) PRIMARY KEY _id SOURCE(MONGODB(HOST '127.0.0.1' PORT 27017 DB 'db' COLLECTION 'c' OPTIONS 'tls=true&appName=keep')) LAYOUT(COMPLEX_KEY_DIRECT());
CREATE TABLE c03 (x String) ENGINE = MongoDB('127.0.0.1:27017', 'db', 'c', 'usr', 'pw', 'tls=true', concat('_', 'id'));
CREATE TABLE c04 (x String) ENGINE = MongoDB('mongodb://127.0.0.1:27017/db', 'c?tlsCertificateKeyFilePassword=KEEPCOLL');
CREATE VIEW c05 AS SELECT * FROM mongodb('127.0.0.1:27017', 'db', 'c', 'usr', 'pw', 'x String', oid_columns = '_id');

SYSTEM RELOAD DICTIONARY d01;
SYSTEM RELOAD DICTIONARY d02;
SYSTEM RELOAD DICTIONARY d03;
SYSTEM RELOAD DICTIONARY d04;
SYSTEM RELOAD DICTIONARY u03;
SYSTEM RELOAD DICTIONARY u05;
SYSTEM RELOAD DICTIONARY c02;

SELECT name, create_table_query FROM system.tables WHERE database = currentDatabase() AND name NOT LIKE 'u%' ORDER BY name;
SELECT name, status, source FROM system.dictionaries WHERE database = currentDatabase() AND name NOT LIKE 'u%' ORDER BY name;

-- The rows with a user and password are checked by the absence of the marker only.
SELECT 'u tables', count(), countIf(create_table_query LIKE '%' || 'OPT' || 'SECRET' || '%')
FROM system.tables WHERE database = currentDatabase() AND name LIKE 'u%';
SELECT 'u dictionaries', countIf(status = 'LOADED'), countIf(source LIKE '%' || 'OPT' || 'SECRET' || '%')
FROM system.dictionaries WHERE database = currentDatabase() AND name LIKE 'u%';

SYSTEM FLUSH LOGS query_log;
SELECT 'logged with a secret', count() FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%' || 'OPT' || 'SECRET' || '%';
-- One finished or failed entry per probe, each with something hidden.
SELECT 'probes logged', count(), countIf(query LIKE '%[HIDDEN]%') FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND log_comment = '${PROBE}' AND type != 'QueryStart';
"

# A table keeps its named collection from being dropped, so the tables go first.
$CLICKHOUSE_CLIENT -m -q "
SET ast_fuzzer_any_query = 0;
DROP NAMED COLLECTION ${NC}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP NAMED COLLECTION ${NC2}; -- { serverError NAMED_COLLECTION_IS_USED }
DROP TABLE e06;
DROP TABLE e07;
DROP TABLE e08;
DROP NAMED COLLECTION ${NC};
DROP NAMED COLLECTION ${NC2};
"
