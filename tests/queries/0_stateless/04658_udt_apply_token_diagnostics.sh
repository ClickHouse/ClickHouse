#!/usr/bin/env bash

set -Eeuo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir=$(mktemp -d "${CLICKHOUSE_TMP}/04658_apply_token.XXXXXX")
guard_table=udt_04658_apply_token_guard
output_file="$work_dir/must_not_exist.tsv"
result=0

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS $guard_table" >/dev/null 2>&1 || true
    rm -r -- "$work_dir"
}
trap cleanup EXIT

record_failure()
{
    local label=$1
    printf '%s\tFAIL\n' "$label"
    result=1
}

run_rejected_query()
{
    local label=$1
    local expected_error=$2
    local expect_hidden=$3
    local opaque_value=$4
    local query=$5
    local mode=${6:-single}
    local stdout_file="$work_dir/$label.stdout"
    local stderr_file="$work_dir/$label.stderr"
    local rc=0

    local -a extra_options=(--allow_experimental_user_defined_types=1)
    if [[ "$mode" == multi ]]; then
        extra_options+=(--multiquery)
    fi

    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT "${extra_options[@]}" --query "$query" > "$stdout_file" 2> "$stderr_file" || rc=$?

    if [[ "$rc" -eq 0 ]]; then
        record_failure "${label}_exit_code"
        return
    fi
    if [[ -s "$stdout_file" ]]; then
        record_failure "${label}_stdout"
        return
    fi
    if ! grep -Fq "$expected_error" "$stderr_file"; then
        record_failure "${label}_error_code"
        return
    fi
    if grep -Fq "$opaque_value" "$stdout_file" "$stderr_file"; then
        record_failure "${label}_value_visible"
        return
    fi
    if [[ "$expect_hidden" -eq 1 ]] && ! grep -Fq '[HIDDEN]' "$stderr_file"; then
        record_failure "${label}_hidden_marker"
        return
    fi

    printf '%s\tOK\n' "$label"
}

run_echo_query()
{
    local label=$1
    local formatted=$2
    local expected_marker=$3
    local opaque_value=$4
    local stdout_file="$work_dir/$label.stdout"
    local stderr_file="$work_dir/$label.stderr"
    local rc=0

    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT \
        --allow_experimental_user_defined_types=1 \
        --echo \
        --echo-formatted="$formatted" \
        --query "PHYSICALIZE TYPE REFERENCES APPLY TOKEN '$opaque_value'" \
        > "$stdout_file" 2> "$stderr_file" || rc=$?

    if [[ "$rc" -eq 0 ]] \
        || ! grep -Fq BAD_ARGUMENTS "$stderr_file" \
        || ! grep -Fq "$expected_marker" "$stdout_file" \
        || ! grep -Fq '[HIDDEN]' "$stderr_file" \
        || grep -Fq "$opaque_value" "$stdout_file" "$stderr_file";
    then
        record_failure "$label"
        return
    fi

    printf '%s\tOK\n' "$label"
}

run_hint_mismatch()
{
    local label=$1
    local opaque_value=$2
    local stdout_file="$work_dir/$label.stdout"
    local stderr_file="$work_dir/$label.stderr"
    local rc=0

    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT \
        --allow_experimental_user_defined_types=1 \
        --multiquery \
        --query "PHYSICALIZE TYPE REFERENCES APPLY TOKEN '$opaque_value'; -- { serverError SYNTAX_ERROR }" \
        > "$stdout_file" 2> "$stderr_file" || rc=$?

    if [[ "$rc" -eq 0 ]] \
        || ! grep -Fq 'Expected server error code' "$stderr_file" \
        || ! grep -Fq '[HIDDEN]' "$stderr_file" \
        || grep -Fq "$opaque_value" "$stdout_file" "$stderr_file";
    then
        record_failure "$label"
        return
    fi

    printf '%s\tOK\n' "$label"
}

$CLICKHOUSE_CLIENT --multiquery --query "
    DROP TABLE IF EXISTS $guard_table;
    CREATE TABLE $guard_table (value UInt8) ENGINE = Memory;
    INSERT INTO $guard_table VALUES (1);
" >/dev/null

before_state=$($CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(value),
        (SELECT count() FROM system.data_type_families WHERE name = 'UDT04658ApplyTokenGuard')
    FROM $guard_table
    FORMAT TSVRaw
")
printf 'before\t%s\n' "$before_state"

truncated_value=udt04658_truncated_opaque_91e3
run_rejected_query \
    truncated SYNTAX_ERROR 1 "$truncated_value" \
    "PHYSICALIZE TYPE REFERENCES APPLY TOKEN '$truncated_value"

malformed_value=udt04658_malformed_opaque_a72c
run_rejected_query \
    malformed SYNTAX_ERROR 1 "$malformed_value" \
    "PHYSICALIZE TYPE REFERENCES APPLY # TOKEN '$malformed_value'"

output_value=udt04658_output_opaque_c44a
run_rejected_query \
    output_tail SYNTAX_ERROR 1 "$output_value" \
    "PHYSICALIZE TYPE REFERENCES APPLY TOKEN '$output_value' INTO OUTFILE '$output_file'"

multi_diagnostic_value=udt04658_multi_diagnostic_opaque_31bd
run_rejected_query \
    multi_diagnostic SYNTAX_ERROR 1 "$multi_diagnostic_value" \
    "SET allow_experimental_user_defined_types = 1;
     PHYSICALIZE TYPE REFERENCES APPLY TOKEN '$multi_diagnostic_value" \
    multi

multi_value=udt04658_multi_opaque_f609
run_rejected_query \
    multi_no_effects BAD_ARGUMENTS 1 "$multi_value" \
    "PHYSICALIZE TYPE REFERENCES APPLY TOKEN '$multi_value';
     INSERT INTO $guard_table VALUES (2);
     SELECT 'must-not-be-written' INTO OUTFILE '$output_file'" \
    multi

raw_echo_value=udt04658_raw_echo_opaque_7d3b
run_echo_query raw_echo 0 '[HIDDEN]' "$raw_echo_value"

formatted_echo_value=udt04658_formatted_echo_opaque_c8a1
run_echo_query formatted_echo 1 '<redacted>' "$formatted_echo_value"

hint_value=udt04658_hint_opaque_0f52
run_hint_mismatch hint_mismatch "$hint_value"

after_state=$($CLICKHOUSE_CLIENT --query "
    SELECT count(), sum(value),
        (SELECT count() FROM system.data_type_families WHERE name = 'UDT04658ApplyTokenGuard')
    FROM $guard_table
    FORMAT TSVRaw
")
printf 'after\t%s\n' "$after_state"

if [[ "$after_state" != "$before_state" ]]; then
    record_failure state_unchanged
else
    printf 'state_unchanged\tOK\n'
fi

if [[ -e "$output_file" ]]; then
    record_failure output_file_absent
else
    printf 'output_file_absent\tOK\n'
fi

exit "$result"
