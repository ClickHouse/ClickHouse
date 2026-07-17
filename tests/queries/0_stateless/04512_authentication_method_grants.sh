#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: user manipulation is not supported there
# Tag no-replicated-database: the test relies on grants on the current database, and CREATE USER is executed on all the replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_04512_${CLICKHOUSE_DATABASE}"
user2="u2_04512_${CLICKHOUSE_DATABASE}"
user3="u3_04512_${CLICKHOUSE_DATABASE}"
user4="u4_04512_${CLICKHOUSE_DATABASE}"
user5="u5_04512_${CLICKHOUSE_DATABASE}"
user6="u6_04512_${CLICKHOUSE_DATABASE}"
user7="u7_04512_${CLICKHOUSE_DATABASE}"
user8="u8_04512_${CLICKHOUSE_DATABASE}"
user9="u9_04512_${CLICKHOUSE_DATABASE}"
user10="u10_04512_${CLICKHOUSE_DATABASE}"
user11="u11_04512_${CLICKHOUSE_DATABASE}"
victim="uv_04512_${CLICKHOUSE_DATABASE}"
role="r_04512_${CLICKHOUSE_DATABASE}"
role2="r2_04512_${CLICKHOUSE_DATABASE}"
role3="r3_04512_${CLICKHOUSE_DATABASE}"

function login()
{
    local password=$1
    shift
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=${password}" -d "$@"
}

function login_expect_error()
{
    local password=$1
    local error=$2
    shift 2
    login "$password" "$@" 2>&1 | grep -m1 -o "$error" | head -n 1
}

function cleanup()
{
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}" -q "DROP USER IF EXISTS ${user2}" -q "DROP USER IF EXISTS ${user3}" -q "DROP USER IF EXISTS ${user4}" -q "DROP USER IF EXISTS ${user5}" -q "DROP USER IF EXISTS ${user6}" -q "DROP USER IF EXISTS ${user7}" -q "DROP USER IF EXISTS ${user8}" -q "DROP USER IF EXISTS ${user9}" -q "DROP USER IF EXISTS ${user10}" -q "DROP USER IF EXISTS ${user11}" -q "DROP USER IF EXISTS ${victim}" -q "DROP ROLE IF EXISTS ${role}" -q "DROP ROLE IF EXISTS ${role2}" -q "DROP ROLE IF EXISTS ${role2}_x" -q "DROP ROLE IF EXISTS ${role3}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}" -q "DROP USER IF EXISTS ${user2}" -q "DROP USER IF EXISTS ${user3}" -q "DROP USER IF EXISTS ${user4}" -q "DROP USER IF EXISTS ${user5}" -q "DROP USER IF EXISTS ${user6}" -q "DROP USER IF EXISTS ${user7}" -q "DROP USER IF EXISTS ${user8}" -q "DROP USER IF EXISTS ${user9}" -q "DROP USER IF EXISTS ${user10}" -q "DROP USER IF EXISTS ${user11}" -q "DROP USER IF EXISTS ${victim}" -q "DROP ROLE IF EXISTS ${role}" -q "DROP ROLE IF EXISTS ${role2}" -q "DROP ROLE IF EXISTS ${role2}_x" -q "DROP ROLE IF EXISTS ${role3}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t1 (x UInt64) ENGINE = MergeTree ORDER BY x" -q "CREATE TABLE t2 (x UInt64) ENGINE = MergeTree ORDER BY x" -q "INSERT INTO t1 VALUES (1)" -q "INSERT INTO t2 VALUES (2)"

# The second authentication method is a 'token': it limits the access rights to a subset of the grants.
# Note that the elements without a database name must be bound to the current database.
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} IDENTIFIED WITH plaintext_password BY 'full_password', plaintext_password BY 'token_password' VALID UNTIL '2077-01-01' GRANTS (SELECT ON t1, INSERT ON t1)"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${role}" -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user}" -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t2 TO ${role}" -q "GRANT ${role} TO ${user}"

echo "-- SHOW CREATE USER shows the GRANTS clause with the database name bound"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${user}" | sed "s/${user}/user/g; s/${CLICKHOUSE_DATABASE}/db/g"

echo "-- system.users exposes the grants of each authentication method"
${CLICKHOUSE_CLIENT} -q "SELECT arrayMap(x -> replaceAll(x, currentDatabase(), 'db'), auth_grants) FROM system.users WHERE name = '${user}'"

echo "-- Login with the full credential: both tables are accessible (t2 via the role)"
login "full_password" "SELECT x FROM t1"
login "full_password" "SELECT x FROM t2"

echo "-- Login with the token: t1 is accessible"
login "token_password" "SELECT x FROM t1"

echo "-- Login with the token: t2 is not accessible (the role rights are limited as well)"
login_expect_error "token_password" "ACCESS_DENIED" "SELECT x FROM t2"

echo "-- Login with the token: INSERT is listed in GRANTS but not granted to the user, so it is denied"
login_expect_error "token_password" "ACCESS_DENIED" "INSERT INTO t1 VALUES (42)"

echo "-- Login with the token: cannot grant its privileges (no grant option after the intersection)"
login_expect_error "token_password" "ACCESS_DENIED" "GRANT SELECT ON t1 TO ${user}"

echo "-- Login with the token: cannot administer roles"
login_expect_error "token_password" "ACCESS_DENIED" "GRANT ${role} TO ${user}"

echo "-- Login with the token: cannot create other tokens (no ALTER USER right)"
login_expect_error "token_password" "ACCESS_DENIED" "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'another_token' GRANTS (SELECT ON t1)"

echo "-- Reattaching to a named session with the token does not reuse the full access rights"
session="04512_session_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=full_password&session_id=${session}" -d "SELECT x FROM t2"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=token_password&session_id=${session}" -d "SELECT x FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user}&password=full_password&session_id=${session}" -d "SELECT x FROM t2"

echo "-- The query result cache must not serve a token the results cached under a broader credential of the same user"
# The full credential populates the query cache for a SELECT on t2 (which the token cannot read).
login "full_password" "SELECT x FROM t2 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0" > /dev/null
# Re-running with the full credential is a cache hit and returns the row, proving the entry is in the cache.
login "full_password" "SELECT x FROM t2 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0"
# The token's GRANTS omit SELECT ON t2, so the same query must be a cache miss and get denied,
# rather than being served the cached rows without an access check.
login_expect_error "token_password" "ACCESS_DENIED" "SELECT x FROM t2 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0"

echo "-- A credential-limited session does not use the query result cache, so a later REVOKE is enforced on the token"
# The token can read t1 (SELECT ON t1 is granted to the user and listed in the token's GRANTS).
# Because the session is credential-limited, this query must not populate the query result cache.
login "token_password" "SELECT x FROM t1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0"
# Revoke the underlying grant from the user.
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t1 FROM ${user}"
# The token must now be denied. The query result cache is not access-control-aware on a hit and is not invalidated by
# REVOKE, so had the token populated it above, this would be served the stale cached row instead of ACCESS_DENIED.
login_expect_error "token_password" "ACCESS_DENIED" "SELECT x FROM t1 SETTINGS use_query_cache = 1, query_cache_min_query_runs = 0"
# Restore the grant so the rest of the test is unaffected.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user}"

echo "-- ALTER USER ADD IDENTIFIED with the GRANTS clause adds a new token"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'second_token' GRANTS (SELECT(x) ON ${CLICKHOUSE_DATABASE}.t2)"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${user}" | sed "s/${user}/user/g; s/${CLICKHOUSE_DATABASE}/db/g"
login "second_token" "SELECT x FROM t2"
login_expect_error "second_token" "ACCESS_DENIED" "SELECT x FROM t1"

echo "-- The GRANTS clause requires a non-empty list of grants in parentheses"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH plaintext_password BY '1' GRANTS ()" 2>&1 | grep -m1 -o "SYNTAX_ERROR" | head -n 1
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH plaintext_password BY '1' GRANTS SELECT ON t1" 2>&1 | grep -m1 -o "SYNTAX_ERROR" | head -n 1

# A GRANTS clause that grants no privileges (e.g. USAGE) is an explicit "deny everything" limit.
# It must not be treated as "no clause" (which would silently give the credential the full user rights).
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user2} IDENTIFIED WITH plaintext_password BY 'full2', plaintext_password BY 'denyall' GRANTS (USAGE ON *.*)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user2}"

echo "-- SHOW CREATE USER keeps the no-privileges clause (it does not collapse to no limit or to unparseable empty parentheses)"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${user2}" | sed "s/${user2}/user2/g"

echo "-- system.users exposes USAGE ON *.* for the deny-all method"
${CLICKHOUSE_CLIENT} -q "SELECT auth_grants FROM system.users WHERE name = '${user2}'"

echo "-- The full credential can read t1"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user2}&password=full2" -d "SELECT x FROM t1"

echo "-- The deny-all token cannot read t1 even though the user can"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user2}&password=denyall" -d "SELECT x FROM t1" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

# Filtered source grants (e.g. READ ON S3('...')) cannot be narrowed by the intersection, because the source
# filter is intersected as an opaque string. They are rejected explicitly instead of silently granting nothing.
echo "-- A filtered source grant in the GRANTS clause is rejected (CREATE USER)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH plaintext_password BY '1' GRANTS (READ ON S3('s3://bucket/private/.*'))" 2>&1 | grep -m1 -o "NOT_IMPLEMENTED" | head -n 1

echo "-- A filtered source grant in the GRANTS clause is rejected (ALTER USER)"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'filtered_token' GRANTS (READ ON S3('s3://bucket/private/.*'))" 2>&1 | grep -m1 -o "NOT_IMPLEMENTED" | head -n 1

# A method verified against an external system (ldap, kerberos, http, jwt) cannot participate in the fail-close
# ambiguity scan at authentication time, because re-checking a credential there would require an extra probe of the
# external system. Another method accepting the same credential could then shadow its GRANTS, so the combination
# is rejected explicitly (fail-close).
echo "-- A GRANTS clause on an externally verified authentication method is rejected (CREATE USER, ldap)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH ldap SERVER 'srv' GRANTS (SELECT ON t1)" 2>&1 | grep -m1 -o "NOT_IMPLEMENTED" | head -n 1

echo "-- A GRANTS clause on an externally verified authentication method is rejected (CREATE USER, kerberos)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH kerberos GRANTS (SELECT ON t1)" 2>&1 | grep -m1 -o "NOT_IMPLEMENTED" | head -n 1

echo "-- A GRANTS clause on an externally verified authentication method is rejected (ALTER USER, http)"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${user} ADD IDENTIFIED WITH http SERVER 'srv' SCHEME 'basic' GRANTS (SELECT ON t1)" 2>&1 | grep -m1 -o "NOT_IMPLEMENTED" | head -n 1

# An element that the regular GRANT statement rejects as not grantable must fail the DDL up front, exactly like
# GRANT does. Otherwise the clause would be displayed as written while the actual session limit silently masks
# the non-grantable flags out.
echo "-- A non-grantable element in the GRANTS clause is rejected (CREATE USER, global privilege on a table)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}_bad IDENTIFIED WITH plaintext_password BY '1' GRANTS (CREATE TEMPORARY TABLE ON t1)" 2>&1 | grep -m1 -o "INVALID_GRANT" | head -n 1

echo "-- A non-grantable element in the GRANTS clause is rejected (ALTER USER, global privilege on a database)"
${CLICKHOUSE_CLIENT} -q "ALTER USER ${user} ADD IDENTIFIED WITH plaintext_password BY 'bad_token' GRANTS (KILL QUERY ON *)" 2>&1 | grep -m1 -o "INVALID_GRANT" | head -n 1

# The GRANTS clause must be serialized precisely. The backward-compatibility rewrites that widen a grant for the
# benefit of older replicas must not apply here: they would broaden a narrow token. With the default
# `enable_read_write_grants = 0`, a plain `GRANT READ ON S3` is dumped as the full `S3` source access, but an
# auth-method token limited to `READ ON S3` must keep its exact, read-only scope (older replicas cannot parse the
# clause at all, so there is nothing to stay compatible with).
echo "-- A source-level grant keeps its exact access type in the GRANTS clause (not widened to the full source)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user3} IDENTIFIED WITH plaintext_password BY 'full3' GRANTS (READ ON S3)"
${CLICKHOUSE_CLIENT} -q "SHOW CREATE USER ${user3}" | sed "s/${user3}/user3/g"
${CLICKHOUSE_CLIENT} -q "SELECT auth_grants FROM system.users WHERE name = '${user3}'"

# Sessions limited by the GRANTS clause cannot administer roles at all (the clause cannot express
# the admin option, so role administration is rejected wholesale, following the fail-close principle).
# This must cover not only granting and revoking roles (the admin option checks) but also the role DDL
# entrypoints, which are authorized with the plain CREATE ROLE / ALTER ROLE / DROP ROLE access types -
# even when these access types are listed in the clause and granted to the user.
echo "-- Role DDL is denied for a limited credential even when CREATE/ALTER/DROP ROLE are listed and granted"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user4} IDENTIFIED WITH plaintext_password BY 'full4', plaintext_password BY 'role_token' GRANTS (CREATE ROLE ON *.*, ALTER ROLE ON *.*, DROP ROLE ON *.*)"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE ROLE, ALTER ROLE, DROP ROLE ON *.* TO ${user4}"
# The full credential of the same user can administer roles.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user4}&password=full4" -d "CREATE ROLE ${role2}"
# The limited credential cannot create, alter or drop roles.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user4}&password=role_token" -d "CREATE ROLE ${role2}_x" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user4}&password=role_token" -d "ALTER ROLE ${role2} RENAME TO ${role2}_x" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user4}&password=role_token" -d "DROP ROLE ${role2}" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
# The full credential can drop the role (which also proves the limited credential did not drop it).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user4}&password=full4" -d "DROP ROLE ${role2}"

# The GRANTS clause limits the effective rights - the set that access checks actually consult, which includes the
# implicit privileges. The implicit expansion (addImplicitAccessRights) is applied to the already-intersected
# access, so only implicit privileges derivable from the listed grants survive: they cannot reintroduce a
# privilege that the intersection removed. In particular, CREATE TEMPORARY TABLE is a global privilege implied
# only by a CREATE TABLE grant; a token limited to SELECT does not imply it, so it stays denied even though the
# user itself is granted it.
echo "-- A token limited to SELECT cannot create a temporary table, though the user (granted CREATE TEMPORARY TABLE) can"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user5} IDENTIFIED WITH plaintext_password BY 'full5', plaintext_password BY 'token5' GRANTS (SELECT ON t1)"
# TABLE ENGINE ON Memory is required too when table_engines_require_grant is enabled (as it is in the CI test config);
# the SELECT-only token gets neither CREATE TEMPORARY TABLE nor TABLE ENGINE ON Memory, so it stays denied regardless.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user5}" -q "GRANT CREATE TEMPORARY TABLE ON *.* TO ${user5}" -q "GRANT TABLE ENGINE ON Memory TO ${user5}"
# The full credential (no GRANTS clause) has CREATE TEMPORARY TABLE and succeeds (no output on success).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user5}&password=full5" -d "CREATE TEMPORARY TABLE tmp5 (x UInt64) ENGINE = Memory"
# The token's SELECT-only intersection does not imply CREATE TEMPORARY TABLE, so the implicit expansion cannot bring it back.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user5}&password=token5" -d "CREATE TEMPORARY TABLE tmp5 (x UInt64) ENGINE = Memory" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

# Authentication returns the first matching method, so a broader method that accepts the same effective credential
# could otherwise shadow a later token-style one. Here both methods are sha256_password of the same plaintext, but the
# CREATE assigns each a different random salt, so a structural comparison would not detect the duplicate. The session
# must still be limited to the intersection of the GRANTS of all matching methods (fail-close), so the broad first
# method cannot restore the rights the token drops.
echo "-- An ambiguous credential (two methods, same password) is limited to the intersection of the matching grants"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user6} IDENTIFIED WITH sha256_password BY 'shared6', sha256_password BY 'shared6' GRANTS (SELECT ON t1)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user6}" -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t2 TO ${user6}"
# The credential is limited to SELECT ON t1 by the second method even though it is accepted by the first (unrestricted) one.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user6}&password=shared6" -d "SELECT x FROM t1"
# t2 is granted to the user, but the token's grants omit it, so it stays denied instead of being served by the broad method.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user6}&password=shared6" -d "SELECT x FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

# When the same credential is accepted by methods with disjoint grants, the intersection grants nothing. This must be
# an explicit deny-all (USAGE ON *.*), not an absent limit that would restore the full user rights.
echo "-- An ambiguous credential with disjoint grants is denied everything (empty intersection is deny-all, not no limit)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user7} IDENTIFIED WITH sha256_password BY 'shared7' GRANTS (SELECT ON t1), sha256_password BY 'shared7' GRANTS (SELECT ON t2)"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user7}" -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t2 TO ${user7}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user7}&password=shared7" -d "SELECT x FROM t1" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user7}&password=shared7" -d "SELECT x FROM t2" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1

# The earliest VALID UNTIL among the methods matching a credential wins even when it has already passed. An expired
# matching method must not silently disappear from the fail-close combination (which would hand the shared credential
# the lifetime of the later method); the credential is expired as a whole, exactly as a single expired method would be.
echo "-- An ambiguous credential where one matching method has already expired is rejected (earliest VALID UNTIL wins)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user8} IDENTIFIED WITH sha256_password BY 'shared8' VALID UNTIL '2000-01-01', sha256_password BY 'shared8' VALID UNTIL '2099-01-01', sha256_password BY 'other8' VALID UNTIL '2099-01-01'"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user8}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user8}&password=shared8" -d "SELECT x FROM t1" 2>&1 | grep -m1 -o "AUTHENTICATION_FAILED" | head -n 1
# A different credential of the same user matches neither of the ambiguous methods, so it is unaffected.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user8}&password=other8" -d "SELECT x FROM t1"

# The same fail-close rule protects the GRANTS limit: when a token method expires, a login with the shared credential
# must be rejected rather than served by the unrestricted method with the full user rights.
echo "-- An expired token method expires the shared credential instead of widening it to the unrestricted method"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user9} IDENTIFIED WITH sha256_password BY 'shared9' VALID UNTIL '2000-01-01' GRANTS (SELECT ON t1), sha256_password BY 'shared9'"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user9}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user9}&password=shared9" -d "SELECT x FROM t1" 2>&1 | grep -m1 -o "AUTHENTICATION_FAILED" | head -n 1

# The fail-close ambiguity scan does not probe an externally verified method (see the GRANTS-on-external-method
# rejection above), so an expired VALID UNTIL configured on such a method is not part of the combination: it is
# a documented limitation (see the GRANTS Clause section of the docs), not a bug, unlike the all-local case above.
echo "-- An externally verified method sharing the credential does not shorten the session (documented limitation)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user10} IDENTIFIED WITH sha256_password BY 'shared10', ldap SERVER 'unused' VALID UNTIL '2000-01-01'"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t1 TO ${user10}"
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user10}&password=shared10" -d "SELECT x FROM t1"

# Administering roles includes changing which roles are activated by default for a user, so a limited credential
# must not be able to run SET DEFAULT ROLE / ALTER USER ... DEFAULT ROLE. Unlike the role DDL above, these are
# authorized with the plain ALTER USER access type, so a token holding ALTER USER would otherwise still be able
# to reconfigure another user's default roles - which is role administration - contradicting the fail-close contract.
echo "-- Default-role administration is denied for a limited credential even when ALTER USER is listed and granted"
${CLICKHOUSE_CLIENT} -q "CREATE ROLE ${role3}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${victim} IDENTIFIED WITH sha256_password BY 'victim_password'"
${CLICKHOUSE_CLIENT} -q "GRANT ${role3} TO ${victim}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user11} IDENTIFIED WITH plaintext_password BY 'full11', plaintext_password BY 'user_token' GRANTS (ALTER USER ON *.*)"
${CLICKHOUSE_CLIENT} -q "GRANT ALTER USER ON *.* TO ${user11}"
# The full credential of the same user can set the victim's default roles (empty output on success).
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user11}&password=full11" -d "SET DEFAULT ROLE ${role3} TO ${victim}"
# The token has ALTER USER after the intersection, but the role-administration guard still denies the default-role change.
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user11}&password=user_token" -d "SET DEFAULT ROLE ${role3} TO ${victim}" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${user11}&password=user_token" -d "ALTER USER ${victim} DEFAULT ROLE ${role3}" 2>&1 | grep -m1 -o "ACCESS_DENIED" | head -n 1
