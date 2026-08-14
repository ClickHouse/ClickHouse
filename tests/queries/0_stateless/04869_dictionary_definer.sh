#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
owner="dict_owner_${CLICKHOUSE_DATABASE}"
creator="dict_creator_${CLICKHOUSE_DATABASE}"
coll_local="coll_local_${CLICKHOUSE_DATABASE}"
coll_remote="coll_remote_${CLICKHOUSE_DATABASE}"
ephemeral="dict_ephemeral_${CLICKHOUSE_DATABASE}"
loader="dict_loader_${CLICKHOUSE_DATABASE}"
constrained="dict_constrained_${CLICKHOUSE_DATABASE}"
renamer="dict_renamer_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
DROP USER IF EXISTS ${owner}, ${creator}, ${ephemeral}, ${loader}, ${constrained}, ${renamer};
DROP NAMED COLLECTION IF EXISTS ${coll_local};
DROP NAMED COLLECTION IF EXISTS ${coll_remote};

CREATE TABLE ${db}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO ${db}.src VALUES (1, 'one'), (2, 'two');
CREATE TABLE ${db}.other (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO ${db}.other VALUES (9, 'nine');

CREATE USER ${owner} IDENTIFIED BY 'first_password';
GRANT SELECT ON ${db}.src TO ${owner};
"

echo '--- 1. the definer replaces stored credentials, so rotating the password does not break the dictionary'
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_definer (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT dictGet('${db}.dict_definer', 'v', toUInt64(1));
ALTER USER ${owner} IDENTIFIED BY 'rotated_password';
SYSTEM RELOAD DICTIONARY ${db}.dict_definer;
SELECT dictGet('${db}.dict_definer', 'v', toUInt64(2));
"

echo '--- 1b. control: with the credentials stored in the source, the same rotation breaks the reload'
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_password (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(USER '${owner}' PASSWORD 'rotated_password' DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0);
SELECT dictGet('${db}.dict_password', 'v', toUInt64(1));
ALTER USER ${owner} IDENTIFIED BY 'rotated_again';
SYSTEM RELOAD DICTIONARY ${db}.dict_password;
" 2>&1 | grep -o -m1 'AUTHENTICATION_FAILED'
${CLICKHOUSE_CLIENT} -m -q "
SYSTEM RELOAD DICTIONARY ${db}.dict_definer;
SELECT 'definer dictionary still loads', dictGet('${db}.dict_definer', 'v', toUInt64(1));
"

echo '--- 2. the load runs with the definer privileges, not as a privileged default user'
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_polp (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'other'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT dictGet('${db}.dict_polp', 'v', toUInt64(9));
" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} -m -q "
GRANT SELECT ON ${db}.other TO ${owner};
SYSTEM RELOAD DICTIONARY ${db}.dict_polp;
SELECT dictGet('${db}.dict_polp', 'v', toUInt64(9));
"

echo '--- 2b. the definer settings constraints bound what the dictionary definition can set'
# A READONLY constraint on the definer must survive the dictionary's own SETTINGS, the same way it
# survives a view's triggering query (04545_definer_settings_readonly_constraint).
${CLICKHOUSE_CLIENT} -q "
CREATE USER ${constrained} IDENTIFIED BY 'constrained_password'
SETTINGS max_memory_usage_for_user = 123456789 READONLY;
"
${CLICKHOUSE_CLIENT} -m -q "
GRANT SELECT ON ${db}.src TO ${constrained};
CREATE DICTIONARY ${db}.dict_constrained (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' QUERY \$\$SELECT 1 AS k, toString(getSetting('max_memory_usage_for_user')) AS v\$\$))
LAYOUT(HASHED())
LIFETIME(0)
SETTINGS(max_memory_usage_for_user = 987654321)
DEFINER = ${constrained} SQL SECURITY DEFINER;
SELECT 'constrained setting wins:', dictGet('${db}.dict_constrained', 'v', toUInt64(1)) = '123456789';
"
# Control: with no constraint on the definer the dictionary's own SETTINGS is applied, so the arm
# above pins the constraint rather than the dictionary SETTINGS being ignored altogether.
${CLICKHOUSE_CLIENT} -m -q "
GRANT SELECT ON ${db}.src TO ${owner};
CREATE DICTIONARY ${db}.dict_unconstrained (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' QUERY \$\$SELECT 1 AS k, toString(getSetting('max_memory_usage_for_user')) AS v\$\$))
LAYOUT(HASHED())
LIFETIME(0)
SETTINGS(max_memory_usage_for_user = 987654321)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT 'unconstrained definer takes the dictionary setting:', dictGet('${db}.dict_unconstrained', 'v', toUInt64(1)) = '987654321';
"

echo '--- 3. SET DEFINER is required to name another user as the definer'
${CLICKHOUSE_CLIENT} -m -q "
CREATE USER ${creator} IDENTIFIED BY 'creator_password';
GRANT CREATE DICTIONARY, SELECT ON ${db}.* TO ${creator};
"
${CLICKHOUSE_CLIENT} --user "${creator}" --password creator_password -q "
CREATE DICTIONARY ${db}.dict_grant (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} -q "GRANT SET DEFINER ON ${owner} TO ${creator}"
${CLICKHOUSE_CLIENT} --user "${creator}" --password creator_password -q "
CREATE DICTIONARY ${db}.dict_grant (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
"
${CLICKHOUSE_CLIENT} -q "SELECT 'created with the grant', definer = '${owner}' FROM system.tables WHERE database = '${db}' AND name = 'dict_grant'"

echo '--- 4. DEFINER = CURRENT_USER is resolved to the creating user'
${CLICKHOUSE_CLIENT} --user "${creator}" --password creator_password -m -q "
CREATE DICTIONARY ${db}.dict_current (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = CURRENT_USER;
"
${CLICKHOUSE_CLIENT} -q "SELECT definer = '${creator}' FROM system.tables WHERE database = '${db}' AND name = 'dict_current'"

echo '--- 5. the clause survives formatting, re-parsing and a detach/attach round trip'
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${db}.dict_definer" | grep -o "DEFINER = ${owner} SQL SECURITY DEFINER" | sed "s/${owner}/<definer>/"
before=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${db}.dict_definer")
${CLICKHOUSE_CLIENT} -m -q "DETACH DICTIONARY ${db}.dict_definer; ATTACH DICTIONARY ${db}.dict_definer;"
after=$(${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${db}.dict_definer")
[ "$before" = "$after" ] && echo 'round trip is stable' || { echo 'round trip changed the definition'; echo "$before"; echo "$after"; }
${CLICKHOUSE_CLIENT} -q "SELECT 'loads after attach', dictGet('${db}.dict_definer', 'v', toUInt64(1))"

echo '--- 5b. a short ATTACH cannot carry the clause, because it has no definition to apply it to'
# The clause is accepted by the parser in this position, so it reaches the interpreter with no
# dictionary definition attached; it is refused with the message that covers every dropped clause.
for clause in "SQL SECURITY DEFINER" "DEFINER = ${owner} SQL SECURITY DEFINER" "SQL SECURITY NONE"
do
    ${CLICKHOUSE_CLIENT} -q "ATTACH DICTIONARY ${db}.dict_attach_clause ${clause}" 2>&1 |
        grep -o -m1 'ATTACH applies the table definition from stored metadata'
    ${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_attach_clause'"
done
# The server has to survive all of it: the reference above is also produced by a dead server, whose
# client errors would not match any of the greps.
${CLICKHOUSE_CLIENT} -q "SELECT 'server is alive', 1"

echo '--- 6. only DEFINER is accepted: INVOKER and NONE are both rejected'
${CLICKHOUSE_CLIENT} -q "
CREATE DICTIONARY ${db}.dict_invoker (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
SQL SECURITY INVOKER;
" 2>&1 | grep -o -m1 "SQL SECURITY INVOKER can't be specified for DICTIONARY"
# NONE would load with no user at all, so it is refused instead of being recorded and ignored. The
# load identity is what makes ignoring it observable: a `NONE` dictionary that was accepted would
# read its source as `default`, which is exactly what a dictionary carrying no clause already does.
${CLICKHOUSE_CLIENT} -q "
CREATE DICTIONARY ${db}.dict_none (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' QUERY 'SELECT 1 AS k, currentUser() AS v'))
LAYOUT(HASHED())
LIFETIME(0)
SQL SECURITY NONE;
" 2>&1 | grep -o -m1 "SQL SECURITY NONE can't be specified for DICTIONARY"
${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_none'"
# Mirror arm: the same source under a DEFINER does load as the definer, so the arm above rejects the
# type rather than the source, and 'v' below would read 'default' if the definer were not honoured.
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_who (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' QUERY 'SELECT 1 AS k, currentUser() AS v'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT 'definer load identity is the definer', dictGet('${db}.dict_who', 'v', toUInt64(1)) = '${owner}';
"
# Control: with no clause the same source keeps loading as the user named in SOURCE, which defaults
# to 'default'. This is the identity a wrongly-accepted NONE would have been indistinguishable from.
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_who_plain (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' QUERY 'SELECT 1 AS k, currentUser() AS v'))
LAYOUT(HASHED())
LIFETIME(0);
SELECT 'no clause load identity is the source user', dictGet('${db}.dict_who_plain', 'v', toUInt64(1)) = 'default';
"

echo '--- 7. a source that cannot honour a definer is rejected by the CREATE itself, and nothing is persisted'
for source in \
    "MYSQL(HOST 'mysql_host' PORT 3306 USER 'u' PASSWORD 'p' DB 'd' TABLE 't')" \
    "HTTP(URL 'http://localhost:1/data' FORMAT 'CSV')" \
    "CLICKHOUSE(HOST 'remote.invalid.example' PORT 9000 DB '${db}' TABLE 'src')" \
    "NULL()"
do
    ${CLICKHOUSE_CLIENT} -q "
    CREATE DICTIONARY ${db}.dict_rejected (k UInt64, v String)
    PRIMARY KEY k
    SOURCE(${source})
    LAYOUT(HASHED())
    LIFETIME(0)
    DEFINER = ${owner} SQL SECURITY DEFINER;
    " 2>&1 | grep -o -m1 'DEFINER is only supported for a dictionary with a local CLICKHOUSE source'
    ${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_rejected'"
    ${CLICKHOUSE_CLIENT} -q "
    CREATE DICTIONARY ${db}.dict_rejected_none (k UInt64, v String)
    PRIMARY KEY k
    SOURCE(${source})
    LAYOUT(HASHED())
    LIFETIME(0)
    SQL SECURITY NONE;
    " 2>&1 | grep -o -m1 "SQL SECURITY NONE can't be specified for DICTIONARY"
    ${CLICKHOUSE_CLIENT} -q "SELECT 'none persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_rejected_none'"
done

echo '--- 7a. a function-valued source parameter is evaluated before locality is decided'
# `PORT tcpPort()` is the documented way to name this server, and a source parameter is only turned
# into a constant during normalization, so deciding locality on the raw definition rejects it.
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_tcpport (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT 'function-valued local port accepted', dictGet('${db}.dict_tcpport', 'v', toUInt64(1));
"
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_hostname (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(HOST hostName() PORT tcpPort() DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT 'function-valued local host accepted', dictGet('${db}.dict_hostname', 'v', toUInt64(2));
"
# Mirror arm: evaluating the parameters must not make every source look local.
${CLICKHOUSE_CLIENT} -q "
CREATE DICTIONARY ${db}.dict_tcpport_remote (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(HOST 'remote.invalid.example' PORT tcpPort() DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
" 2>&1 | grep -o -m1 'DEFINER is only supported for a dictionary with a local CLICKHOUSE source'
${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_tcpport_remote'"

echo '--- 7f. an unqualified name is judged the same way, before the current database is filled in'
# The gate runs before `create.setDatabase(current_database)`, so it sees an empty database name.
${CLICKHOUSE_CLIENT} -m -q "
USE ${db};
CREATE DICTIONARY dict_unqualified (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT 'unqualified local accepted:', dictGet('${db}.dict_unqualified', 'v', toUInt64(1));
"
${CLICKHOUSE_CLIENT} -m -q "
USE ${db};
CREATE DICTIONARY dict_unqualified_remote (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(HOST 'remote.invalid.example' PORT 9000 DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
" 2>&1 | grep -o -m1 'DEFINER is only supported for a dictionary with a local CLICKHOUSE source'
${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_unqualified_remote'"

echo '--- 7b. locality is decided on the effective configuration, so a named collection is resolved first'
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED COLLECTION ${coll_remote} AS host = 'remote.invalid.example', port = 9000;
CREATE NAMED COLLECTION ${coll_local} AS host = 'localhost';
"
${CLICKHOUSE_CLIENT} -q "
CREATE DICTIONARY ${db}.dict_coll_remote (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(NAME ${coll_remote} DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
" 2>&1 | grep -o -m1 'DEFINER is only supported for a dictionary with a local CLICKHOUSE source'
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_coll_local (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(NAME ${coll_local} DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
SELECT 'local collection accepted', dictGet('${db}.dict_coll_local', 'v', toUInt64(1));
"

echo '--- 7c. a rejected CREATE leaves no ephemeral definer account behind'
# An ephemeral definer is the only shape that makes this observable: resolving it inserts a real
# no-authentication `<user>:definer` account that nothing would collect after a rejection.
${CLICKHOUSE_CLIENT} -m -q "
CREATE USER OR REPLACE ${ephemeral} IN memory IDENTIFIED BY 'ephemeral_password';
GRANT SELECT ON ${db}.src TO ${ephemeral};
"
${CLICKHOUSE_CLIENT} -q "SELECT 'definer is ephemeral:', storage FROM system.users WHERE name = '${ephemeral}'"
${CLICKHOUSE_CLIENT} -q "
CREATE DICTIONARY ${db}.dict_ephemeral (k UInt64, v String)
PRIMARY KEY k
SOURCE(MYSQL(HOST 'mysql_host' PORT 3306 USER 'u' PASSWORD 'p' DB 'd' TABLE 't'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${ephemeral} SQL SECURITY DEFINER;
" >/dev/null 2>&1
${CLICKHOUSE_CLIENT} -q "SELECT 'definer accounts after the rejection:', count() FROM system.users WHERE name = '${ephemeral}:definer'"
# Control: an accepted CREATE with the same ephemeral definer does resolve it, so the count above
# is zero because the rejection happened first, not because the account is never created.
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_ephemeral_ok (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${ephemeral} SQL SECURITY DEFINER;
"
${CLICKHOUSE_CLIENT} -q "SELECT 'definer accounts after an accepted create:', count() FROM system.users WHERE name = '${ephemeral}:definer'"
${CLICKHOUSE_CLIENT} -q "SELECT 'loads as the ephemeral definer:', dictGet('${db}.dict_ephemeral_ok', 'v', toUInt64(1))"

echo '--- 7d. a rejected CREATE does not keep its named collection alive'
# Positive control: a collection a live dictionary uses is refused, so the silence below is a
# measured absence rather than a grep that can never match. A dependency registered for a
# dictionary that was rejected is collected here by the pre-existing stale-entry filter in
# InterpreterDropNamedCollectionQuery, so the collection is droppable either way.
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${coll_local}" 2>&1 | grep -o -m1 'NAMED_COLLECTION_IS_USED'
${CLICKHOUSE_CLIENT} -q "DROP NAMED COLLECTION ${coll_remote}" 2>&1 | grep -o -m1 'NAMED_COLLECTION_IS_USED'
${CLICKHOUSE_CLIENT} -q "SELECT 'collection dropped:', count() = 0 FROM system.named_collections WHERE name = '${coll_remote}'"

echo '--- 7e. the collection is still authorized, so an unprivileged creator cannot resolve it'
${CLICKHOUSE_CLIENT} --user "${creator}" --password creator_password -q "
CREATE DICTIONARY ${db}.dict_coll_denied (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(NAME ${coll_local} DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} -q "GRANT NAMED COLLECTION ON ${coll_local} TO ${creator}"
${CLICKHOUSE_CLIENT} --user "${creator}" --password creator_password -q "
CREATE DICTIONARY ${db}.dict_coll_denied (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(NAME ${coll_local} DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER;
"
${CLICKHOUSE_CLIENT} -q "SELECT 'created with the collection grant:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_coll_denied'"

echo '--- 8. a dictionary with no clause keeps its current definition and behaviour'
${CLICKHOUSE_CLIENT} -m -q "
CREATE DICTIONARY ${db}.dict_plain (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0);
SELECT position(create_table_query, 'SQL SECURITY') = 0, definer = '' FROM system.tables WHERE database = '${db}' AND name = 'dict_plain';
SELECT dictGet('${db}.dict_plain', 'v', toUInt64(1));
"
${CLICKHOUSE_CLIENT} -m -q "
SET default_normal_view_sql_security = 'DEFINER';
CREATE DICTIONARY ${db}.dict_plain_default (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0);
SELECT position(create_table_query, 'SQL SECURITY') = 0, definer = '' FROM system.tables WHERE database = '${db}' AND name = 'dict_plain_default';
SELECT dictGet('${db}.dict_plain_default', 'v', toUInt64(1));
"

echo '--- 8c. a clause carrying no explicit type is judged by the type it will be given'
# `processSQLSecurityOption` substitutes `default_normal_view_sql_security` for an unset type, so a
# clause that arrives without one still has to be rejected when that default is INVOKER. The AST JSON
# dialect is the only way to write such a clause: `ParserSQLSecurity` never produces an unset type.
json=$(${CLICKHOUSE_CLIENT} --format=TSVRaw -q "SELECT parseQueryToJSON(\$\$
CREATE DICTIONARY ${db}.dict_json (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
SQL SECURITY INVOKER\$\$)")
${CLICKHOUSE_CLIENT} --dialect=clickhouse_json --enable_json_ast_dialect=1 -q "${json}" 2>&1 |
    grep -o -m1 "SQL SECURITY INVOKER can't be specified for DICTIONARY"
json_no_type=${json//,\"security_type\":\"INVOKER\"/}
${CLICKHOUSE_CLIENT} --dialect=clickhouse_json --enable_json_ast_dialect=1 -q "${json_no_type}" 2>&1 |
    grep -o -m1 "SQL SECURITY INVOKER can't be specified for DICTIONARY"
${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_json'"
# Mirror arm: the same typeless clause resolves to DEFINER under that default, which a local source
# can honour, so it must be accepted. Without this the arm also passes a gate that rejects every
# typeless clause outright.
${CLICKHOUSE_CLIENT} --dialect=clickhouse_json --enable_json_ast_dialect=1 \
    --default_normal_view_sql_security=DEFINER --default_view_definer="${owner}" \
    -q "${json_no_type//dict_json/dict_json_definer}"
${CLICKHOUSE_CLIENT} -q "SELECT 'definer default accepted:', definer = '${owner}' FROM system.tables WHERE database = '${db}' AND name = 'dict_json_definer'"

echo '--- 8e. a DEFINER clause that names nobody is rejected, because nothing later fills the identity in'
# `ASTSQLSecurity::readJSON` accepts a DEFINER type with no definer child, and
# `processSQLSecurityOption` leaves that shape alone, so the load would authenticate the user given
# in SOURCE while the metadata advertises a definer.
json_definer=$(${CLICKHOUSE_CLIENT} --format=TSVRaw -q "SELECT parseQueryToJSON(\$\$
CREATE DICTIONARY ${db}.dict_no_definer (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${owner} SQL SECURITY DEFINER\$\$)")
definer_object=",\"definer\":{\"type\":\"UserNameWithHost\",\"username\":{\"type\":\"Identifier\",\"name\":\"${owner}\"}}"
json_empty_definer=${json_definer/"${definer_object}"/}
# The substitution has to have removed something, or the arm would just re-run the mirror below.
[ "$json_empty_definer" != "$json_definer" ] && echo 'definer identity removed from the payload' || echo 'FAILED to remove the definer identity'
${CLICKHOUSE_CLIENT} --dialect=clickhouse_json --enable_json_ast_dialect=1 -q "${json_empty_definer}" 2>&1 |
    grep -o -m1 'SQL SECURITY DEFINER for DICTIONARY requires a definer'
${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_no_definer'"
# The server has to survive it: dereferencing the absent identity aborts the process, and a dead
# server's client errors match none of the greps above.
${CLICKHOUSE_CLIENT} -q "SELECT 'server alive:', 1"
# Mirror arm: the same payload with the identity still in it is accepted, so the arm rejects the
# missing definer and not the JSON route.
${CLICKHOUSE_CLIENT} --dialect=clickhouse_json --enable_json_ast_dialect=1 -q "${json_definer}"
${CLICKHOUSE_CLIENT} -q "SELECT 'payload with an identity accepted:', definer = '${owner}' FROM system.tables WHERE database = '${db}' AND name = 'dict_no_definer'"

echo '--- 8d. a create whose eager load fails leaves no ephemeral definer account behind'
# The eager load throws after the storage was constructed, so the dependency it registered is only
# released by the rollback guard. Releasing the last dependency of a `<user>:definer` clone is also
# what deletes that no-authentication account, which is the observable side of the guard.
${CLICKHOUSE_CLIENT} -m -q "
CREATE USER OR REPLACE ${loader} IN memory IDENTIFIED BY 'loader_password';
"
${CLICKHOUSE_CLIENT} -q "
CREATE DICTIONARY ${db}.dict_eager (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
SETTINGS(dictionary_lazy_load = 0)
DEFINER = ${loader} SQL SECURITY DEFINER;
" 2>&1 | grep -o -m1 'ACCESS_DENIED'
${CLICKHOUSE_CLIENT} -q "SELECT 'persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_eager'"
${CLICKHOUSE_CLIENT} -q "SELECT 'definer accounts after the failed load:', count() FROM system.users WHERE name = '${loader}:definer'"
# The rollback runs while the load's exception is in flight, so an exception escaping it would
# terminate the server; a dead server's client errors match none of the greps above.
${CLICKHOUSE_CLIENT} -q "SELECT 'server alive after the rollback:', 1"

echo '--- 9. the definer cannot be dropped while a dictionary uses it'
${CLICKHOUSE_CLIENT} -q "DROP USER ${owner}" 2>&1 | grep -o -m1 'HAVE_DEPENDENT_OBJECTS'
${CLICKHOUSE_CLIENT} -m -q "
DROP DICTIONARY ${db}.dict_definer;
DROP DICTIONARY ${db}.dict_polp;
DROP DICTIONARY ${db}.dict_grant;
DROP DICTIONARY ${db}.dict_coll_local;
DROP DICTIONARY ${db}.dict_coll_denied;
DROP DICTIONARY ${db}.dict_json_definer;
DROP DICTIONARY ${db}.dict_who;
DROP DICTIONARY ${db}.dict_tcpport;
DROP DICTIONARY ${db}.dict_hostname;
DROP DICTIONARY ${db}.dict_no_definer;
DROP DICTIONARY ${db}.dict_unconstrained;
DROP DICTIONARY ${db}.dict_unqualified;
"
${CLICKHOUSE_CLIENT} -q "DROP USER ${owner}"
${CLICKHOUSE_CLIENT} -q "SELECT 'definer dropped:', count() = 0 FROM system.users WHERE name = '${owner}'"
# Dropping the last dictionary of an ephemeral definer must also collect its `<user>:definer` clone,
# which only happens if the storage released the dependency on drop.
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY ${db}.dict_ephemeral_ok"
${CLICKHOUSE_CLIENT} -q "SELECT 'ephemeral definer clone collected:', count() = 0 FROM system.users WHERE name = '${ephemeral}:definer'"

echo '--- 9b. a dictionary that gains a UUID by being renamed still protects its definer'
# A dependency is keyed on the UUID, and an Ordinary database has none, so moving into an Atomic one
# is the point at which the dictionary becomes trackable.
ord_db="${CLICKHOUSE_DATABASE}_ord"
atom_db="${CLICKHOUSE_DATABASE}_atom"
# Creating a database with the Ordinary engine emits a server warning, which is silenced the way
# 02988_ordinary_database_warning does.
${CLICKHOUSE_CLIENT} -m -q "
DROP DATABASE IF EXISTS ${ord_db};
DROP DATABASE IF EXISTS ${atom_db};
SET send_logs_level = 'fatal';
SET allow_deprecated_database_ordinary = 1;
CREATE DATABASE ${ord_db} ENGINE = Ordinary;
CREATE DATABASE ${atom_db} ENGINE = Atomic;
CREATE TABLE ${ord_db}.src (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO ${ord_db}.src VALUES (1, 'one');
CREATE USER ${renamer} IDENTIFIED BY 'renamer_password';
GRANT SELECT ON ${ord_db}.src TO ${renamer};
CREATE DICTIONARY ${ord_db}.dict_renamed (k UInt64, v String)
PRIMARY KEY k
SOURCE(CLICKHOUSE(DB '${ord_db}' TABLE 'src'))
LAYOUT(HASHED())
LIFETIME(0)
DEFINER = ${renamer} SQL SECURITY DEFINER;
"
# Control: while the dictionary is still in the Ordinary database there is no UUID to key a dependency
# on, so the definer is droppable. Recreating it under the same name keeps the stored definer valid.
${CLICKHOUSE_CLIENT} -q "DROP USER ${renamer}"
${CLICKHOUSE_CLIENT} -m -q "
CREATE USER ${renamer} IDENTIFIED BY 'renamer_password';
GRANT SELECT ON ${ord_db}.src TO ${renamer};
SELECT 'untracked definer was droppable:', 1;
"
${CLICKHOUSE_CLIENT} -q "RENAME DICTIONARY ${ord_db}.dict_renamed TO ${atom_db}.dict_renamed"
${CLICKHOUSE_CLIENT} -m -q "
SELECT 'renamed into a UUID database:', uuid != toUUID('00000000-0000-0000-0000-000000000000') FROM system.tables WHERE database = '${atom_db}' AND name = 'dict_renamed';
SELECT 'loads after the rename:', dictGet('${atom_db}.dict_renamed', 'v', toUInt64(1));
"
${CLICKHOUSE_CLIENT} -q "DROP USER ${renamer}" 2>&1 | grep -o -m1 'HAVE_DEPENDENT_OBJECTS'
# Without the dependency the drop above succeeds and every later reload fails UNKNOWN_USER, so assert
# the definer is still there and the dictionary still reloads.
${CLICKHOUSE_CLIENT} -m -q "
SELECT 'definer survived the drop attempt:', count() FROM system.users WHERE name = '${renamer}';
SYSTEM RELOAD DICTIONARY ${atom_db}.dict_renamed;
SELECT 'reloads with the definer:', dictGet('${atom_db}.dict_renamed', 'v', toUInt64(1));
"
${CLICKHOUSE_CLIENT} -m -q "
DROP DICTIONARY ${atom_db}.dict_renamed;
DROP USER ${renamer};
DROP DATABASE ${ord_db};
DROP DATABASE ${atom_db};
"

echo '--- 10. a table over a dictionary still rejects the clause, because that engine cannot enforce it'
# The definer must be a user that still exists here: an unknown one fails earlier with UNKNOWN_USER
# and never reaches the engine feature check this arm exists to pin.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE ${db}.dict_as_table (k UInt64, v String) ENGINE = Dictionary('${db}.dict_plain')
DEFINER = ${creator} SQL SECURITY DEFINER;
" 2>&1 | grep -o -m1 "Engine Dictionary doesn't support SQL SECURITY clause"
${CLICKHOUSE_CLIENT} -q "SELECT 'table over a dictionary persisted:', count() FROM system.tables WHERE database = '${db}' AND name = 'dict_as_table'"

${CLICKHOUSE_CLIENT} -m -q "
DROP DICTIONARY IF EXISTS ${db}.dict_current;
DROP DICTIONARY IF EXISTS ${db}.dict_constrained;
DROP NAMED COLLECTION IF EXISTS ${coll_local};
DROP USER IF EXISTS ${owner}, ${creator}, ${ephemeral}, ${loader}, ${constrained}, ${renamer};
"
