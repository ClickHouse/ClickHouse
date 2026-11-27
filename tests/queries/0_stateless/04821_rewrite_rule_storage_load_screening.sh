#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Rules loaded from persisted storage bypass the CREATE RULE / ALTER RULE template screening,
# so a source template written directly into the storage (or persisted before a screening rule
# was introduced) must be re-screened on load: it must stay visible and droppable, but a query
# that requests it via `query_rules` must fail instead of the rule silently over-matching or,
# worse, a REJECT rule silently not firing. Uses `clickhouse local` with a private path so the
# persisted rule file can be tampered with between sessions.

WORKDIR="${CLICKHOUSE_TMP}/04821_rewrite_rule_storage_load_screening_${CLICKHOUSE_DATABASE}"
rm -rf "${WORKDIR}"
mkdir -p "${WORKDIR}"

# Session 1: persist a valid rule.
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --query "CREATE RULE r_load AS (SELECT 1) REJECT WITH 'nope'"

# Tamper with the persisted rule: an `ASTCreateQuery` source template is not on the audited
# allowlist and would be rejected by CREATE RULE, but the storage file is read back verbatim.
echo "CREATE RULE r_load AS (CREATE TABLE t0 (x Int32) ENGINE = Memory) REJECT WITH 'nope'" > "${WORKDIR}/query_rules/r_load.sql"

# Session 2: the tampered rule is loaded but deactivated (fail closed).
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --multiquery "
-- The rule is still visible.
SELECT count() FROM system.query_rules WHERE name = 'r_load';
-- A query that does not request the rule is unaffected.
SELECT 2;
" 2>&1

# A query that requests the deactivated rule fails with a clear error instead of applying it.
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --multiquery "SET query_rules = 'r_load'; SELECT 3;" 2>&1 \
    | grep -o -m1 "failed template validation"

# The deactivated rule can still be dropped, resolving the situation.
${CLICKHOUSE_LOCAL} --path "${WORKDIR}" --multiquery "
DROP RULE r_load;
SELECT count() FROM system.query_rules WHERE name = 'r_load';
"

rm -rf "${WORKDIR}"
