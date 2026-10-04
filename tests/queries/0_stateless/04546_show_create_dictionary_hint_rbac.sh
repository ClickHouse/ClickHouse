#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SHOW CREATE DICTIONARY` is authorized with the `SHOW DICTIONARIES` grant, which does not
# imply `SHOW TABLES`. The "Maybe you meant ...?" hint for a missing dictionary must work for
# a user with dictionary-only visibility, and it must suggest only dictionaries to such a
# user - never tables, whose existence the user is not allowed to see.
#
# The test database name is dynamic, so it is normalized to `{db}` in the output.
DB="${CLICKHOUSE_DATABASE}"
USER="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.secret_target (key UInt64, val UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE DICTIONARY ${DB}.dict_target (key UInt64 DEFAULT 0, val UInt64 DEFAULT 0) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'secret_target' DB '${DB}')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${USER}"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW DICTIONARIES ON ${DB}.* TO ${USER}"

# The dictionary-only user gets a hint about the similarly-named dictionary.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.dict_targe" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.dict_target?" | sed "s/${DB}/{db}/g"

# But tables stay invisible to that user: a typo close to the table's name must produce
# no hint at all (0 matching lines), otherwise the hint would leak the table's existence.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.secret_targe" 2>&1 \
    | grep -c -F "Maybe you meant" || true

# Sanity check: for a user with full visibility the same typo does produce the table hint.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${DB}.secret_targe" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.secret_target?" | sed "s/${DB}/{db}/g"

# It is not only the typo hint that must not leak the table: an *exact* probe must not either.
# `SHOW CREATE DICTIONARY ${DB}.secret_target` names a real regular table, not a dictionary. For a
# dictionary-only user this must be reported as a missing dictionary - identical to a name that does
# not exist - never as "... is not a DICTIONARY", which would confirm the hidden table exists.
# (grep -m1 -c: with --send_logs_level the server may also echo the exception as a log event, so the
# message can appear more than once; cap the count at a single match to stay deterministic.)
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.secret_target" 2>&1 \
    | grep -m1 -c -F "is not a DICTIONARY" || true
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.secret_target" 2>&1 \
    | grep -m1 -c -F "There is no dictionary" || true

# Sanity check: a user with full visibility keeps the precise "is not a DICTIONARY" diagnostic.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${DB}.secret_target" 2>&1 \
    | grep -m1 -c -F "is not a DICTIONARY" || true

# A closer *hidden table* must not mask a farther *visible dictionary* for a dictionary-only user.
# `mask_target` (a table) is one edit away from the typo `mask_targe`; `mask_targets` (a dictionary)
# is two. The visibility check must look past the closer table and still suggest the dictionary,
# rather than giving up and returning the bare error.
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.mask_target (key UInt64, val UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "CREATE DICTIONARY ${DB}.mask_targets (key UInt64 DEFAULT 0, val UInt64 DEFAULT 0) PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'mask_target' DB '${DB}')) LIFETIME(MIN 0 MAX 0) LAYOUT(FLAT())"

# The dictionary-only user is pointed at the visible dictionary, not left with no hint.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.mask_targe" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.mask_targets?" | sed "s/${DB}/{db}/g"

# And the closer hidden table's name must never leak into the hint for that user.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.mask_targe" 2>&1 \
    | grep -c -F "meant ${DB}.mask_target?" || true

# Sanity check: a user with full visibility gets the closest match, which is the table.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${DB}.mask_targe" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.mask_target?" | sed "s/${DB}/{db}/g"

# The same masking must be avoided for an *exact* hidden-table probe, not only for a typo. A
# dictionary-only user naming the hidden table `mask_target` exactly is remasked to a missing
# dictionary; the hint search must skip that exact (hidden) name and keep scanning the same database,
# so it still offers the visible dictionary `mask_targets` (one edit away) instead of giving up and
# returning the bare error.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.mask_target" 2>&1 \
    | grep -oF -m1 "Maybe you meant ${DB}.mask_targets?" | sed "s/${DB}/{db}/g"

# The exact hidden table's own name must never leak into that hint.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.mask_target" 2>&1 \
    | grep -c -F "meant ${DB}.mask_target?" || true

# And the exact probe must stay indistinguishable from a missing dictionary - reported as
# "There is no dictionary", never "... is not a DICTIONARY", which would confirm the table exists.
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.mask_target" 2>&1 \
    | grep -m1 -c -F "is not a DICTIONARY" || true
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.mask_target" 2>&1 \
    | grep -m1 -c -F "There is no dictionary" || true

# Sanity check: a user with full visibility keeps the precise "is not a DICTIONARY" diagnostic.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${DB}.mask_target" 2>&1 \
    | grep -m1 -c -F "is not a DICTIONARY" || true

# TOCTOU end-state: if a dictionary is replaced by a regular table under the same name, a
# dictionary-only user querying that name must still see it as a missing dictionary, never as
# "... is not a DICTIONARY". The fix fetches and validates the create query in a single lookup, so
# the object's kind at fetch time - a regular table here - is reported consistently, whatever it was
# before. This is the settled state a concurrent drop-of-dictionary/create-of-table would leave, and
# it must be indistinguishable from an exact-name table probe or a missing name for that user.
${CLICKHOUSE_CLIENT} -q "DROP DICTIONARY ${DB}.dict_target"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${DB}.dict_target (key UInt64, val UInt64) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.dict_target" 2>&1 \
    | grep -m1 -c -F "is not a DICTIONARY" || true
${CLICKHOUSE_CLIENT} --user "${USER}" -q "SHOW CREATE DICTIONARY ${DB}.dict_target" 2>&1 \
    | grep -m1 -c -F "There is no dictionary" || true

# Sanity check: a user with full visibility still gets the precise "is not a DICTIONARY" diagnostic.
${CLICKHOUSE_CLIENT} -q "SHOW CREATE DICTIONARY ${DB}.dict_target" 2>&1 \
    | grep -m1 -c -F "is not a DICTIONARY" || true

${CLICKHOUSE_CLIENT} -q "DROP USER ${USER}"
