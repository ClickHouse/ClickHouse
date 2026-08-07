#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: the arms DETACH and re-ATTACH a table to replay its on-disk metadata; a
# Replicated database turns DETACH into DETACH PERMANENTLY and routes each ATTACH through a 3-host
# DDL whose timeout would mask the error being asserted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `remote`/`remoteSecure`/`Remote`/`RemoteSecure` call whose first argument names a named
# collection that does not exist must report the missing collection. It used to silently fall
# through to positional parsing, which reinterpreted `key = value` overrides as data expressions
# and reported an unrelated error (`Function with name \`merge\` does not exist`, or
# `Unknown expression or function identifier \`sharding_key\``). Because ATTACH replays on-disk
# metadata through the same parser as startup metadata loading, the misdirecting error surfaced as
# a server that would not start.
#
# `remote(<identifier>, ...)` also legitimately accepts a *cluster* name, so the fall-through must
# keep working for that form; the arms below pin both directions.
#
# Named collections are server-global, so the collection names are scoped to the (unique) test
# database to avoid collisions across concurrent runs. A table left DETACHED while its collection
# is missing keeps on-disk metadata that cannot be loaded, so each such table is re-attached and
# dropped as soon as its assertion is made: a restart between the assertion and the cleanup is
# what turned this defect into a startup failure in the first place.
NC_TF="nc_missing_tf_${CLICKHOUSE_DATABASE}"
NC_SK="nc_missing_sk_${CLICKHOUSE_DATABASE}"
NC_LIVE="nc_live_${CLICKHOUSE_DATABASE}"
NC_NEVER="nc_never_created_${CLICKHOUSE_DATABASE}"

# Report the error code and whether the message names the collection, rather than the full text:
# the collection name embeds the test database, so it must not reach the reference file.
run_and_classify() {
    local label="$1" collection="$2" query="$3" out code names
    out=$(${CLICKHOUSE_CLIENT} --query "${query}" 2>&1)
    code=$(echo "${out}" | grep -oE 'Code: [0-9]+' | head -1)
    if echo "${out}" | grep -qF "\`${collection}\`"; then names='yes'; else names='no'; fi
    echo "${label} ${code:-no error} names_collection=${names}"
}

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE nc_target (x UInt8) ENGINE = Memory;
INSERT INTO nc_target VALUES (1), (2), (3);

CREATE NAMED COLLECTION ${NC_TF} AS host = '127.0.0.1';
CREATE NAMED COLLECTION ${NC_SK} AS host = '127.0.0.1', db = '${CLICKHOUSE_DATABASE}', \`table\` = 'nc_target';
CREATE NAMED COLLECTION ${NC_LIVE} AS host = '127.0.0.1', db = '${CLICKHOUSE_DATABASE}', \`table\` = 'nc_target';
"

# (a) The CI carrier: the Remote engine with a table-function target. ATTACH replays the on-disk
#     metadata, which is the same path startup metadata loading takes.
${CLICKHOUSE_CLIENT} --query "
-- The arm needs the collection to be missing while the table's metadata still references it, which
-- is the state the dependency guard exists to prevent, so this arm opts out of the guard.
SET check_named_collection_dependencies = 0;
CREATE TABLE t_engine_tf ENGINE = Remote(${NC_TF}, database = merge(currentDatabase(), '^nc_target\$'));
SELECT count() FROM t_engine_tf;
DETACH TABLE t_engine_tf SYNC;
DROP NAMED COLLECTION ${NC_TF};
"
run_and_classify '(a) engine, database = merge(...):' "${NC_TF}" "ATTACH TABLE t_engine_tf"
# Recreating the collection makes the very same metadata attach and read, so the persisted
# definition was always correct and only the missing collection made it unloadable.
${CLICKHOUSE_CLIENT} --query "
CREATE NAMED COLLECTION ${NC_TF} AS host = '127.0.0.1';
ATTACH TABLE t_engine_tf;
SELECT count() FROM t_engine_tf;
DROP TABLE t_engine_tf;
DROP NAMED COLLECTION ${NC_TF};
"

# (b) The same defect with a non-table-function override, which used to report a different
#     unrelated error: the fix is the fall-through itself, not a `merge`-specific special case.
${CLICKHOUSE_CLIENT} --query "
SET check_named_collection_dependencies = 0;
CREATE TABLE t_engine_sk ENGINE = Remote(${NC_SK}, sharding_key = rand());
SELECT count() FROM t_engine_sk;
DETACH TABLE t_engine_sk SYNC;
DROP NAMED COLLECTION ${NC_SK};
"
run_and_classify '(b) engine, sharding_key = rand():' "${NC_SK}" "ATTACH TABLE t_engine_sk"
${CLICKHOUSE_CLIENT} --query "
CREATE NAMED COLLECTION ${NC_SK} AS host = '127.0.0.1', db = '${CLICKHOUSE_DATABASE}', \`table\` = 'nc_target';
ATTACH TABLE t_engine_sk;
DROP TABLE t_engine_sk;
DROP NAMED COLLECTION ${NC_SK};
"

# The `remote` table function shares the parser with the engine, so one fix covers both.
run_and_classify '(c) table function, database = merge(...):' "${NC_NEVER}" \
    "SELECT count() FROM remote(${NC_NEVER}, database = merge(currentDatabase(), '^nc_target\$'))"
run_and_classify '(d) table function, sharding_key = rand():' "${NC_NEVER}" \
    "SELECT count() FROM remote(${NC_NEVER}, sharding_key = rand())"

# (e) An identifier first argument also names a cluster, and that form must keep reaching the
#     positional parser.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM remote(test_shard_localhost, currentDatabase(), nc_target)"

# (f) A positional sharding key may itself be an equality expression, so only the *second*
#     argument being a \`key = value\` pair marks a named-collection call.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM remote(test_shard_localhost, currentDatabase(), 'nc_target', x = 1)"

# (g) A misspelled cluster name still reports a missing cluster, not a missing collection.
run_and_classify '(g) unknown cluster, no override:' 'no_such_cluster' \
    "SELECT count() FROM remote(no_such_cluster, currentDatabase(), nc_target)"

# (h) A live collection is unaffected, and the definition still round-trips.
${CLICKHOUSE_CLIENT} --query "
SELECT count() FROM remote(${NC_LIVE}, sharding_key = rand());
CREATE TABLE t_live ENGINE = Remote(${NC_LIVE}, sharding_key = rand());
SELECT count() FROM t_live;
"
${CLICKHOUSE_CLIENT} --query "SHOW CREATE TABLE t_live" | sed "s/${NC_LIVE}/NC_LIVE/g"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE t_live;
DROP NAMED COLLECTION ${NC_LIVE};
DROP TABLE nc_target;
"
