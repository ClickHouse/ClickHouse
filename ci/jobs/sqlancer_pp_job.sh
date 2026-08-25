#!/bin/bash

# SQLancer++ check.
#
# Runs https://github.com/suyZhong/SQLancerPlusPlus against a freshly started
# clickhouse-server, exercising its built-in `general` provider with the four
# oracles it ships (NoREC, WHERE, QUERY_PARTITIONING, FUZZING). The shared image
# (`clickhouse/sqlancer-test`, also used by the SQLancer job) bakes a SQLancer++
# build under JDK 25 whose ClickHouse JDBC dependency has been swapped to
# `com.clickhouse:clickhouse-jdbc` 0.9.8 (client-v2 transport), talking to
# clickhouse-server's HTTP port (8123).
#
# Mirrors `sqlancer_job.sh` in shape so the praktika report consumer remains
# happy: emits a `result_<normalized_job_name>.json` with one entry per oracle
# plus attached log files.

set -exu

# Capture the job start timestamp so the result file can report a real
# `start_time`/`duration`. Praktika's CIDB inserter rejects a `null` `start_time`
# (it calls `datetime.utcfromtimestamp(start_time)`, which fails on `None`).
JOB_START_TIME=$(date +%s)

REPO_DIR=$(readlink -f .)
TMP_PATH=$(readlink -f ./ci/tmp/)
OUTPUT_PATH="$TMP_PATH/sqlancer_pp_output"
PID_FILE="$TMP_PATH/clickhouse-server.pid"
CLICKHOUSE_BIN="$TMP_PATH/clickhouse"

# Praktika reads the job result from `./ci/tmp/result_<normalized_job_name>.json`,
# where the normalization matches `Utils.normalize_string` in `ci/praktika/utils.py`
# (see `sqlancer_job.sh`, which does the same). Writing a plain `result.json` here
# is what made Praktika report ERROR "Job killed or terminated, no Result provided"
# and drop every oracle result and attached log - the job's real FAIL/OK status
# was written to a file Praktika never reads. `JOB_NAME` is not propagated into
# the docker container, so read it from the serialized environment Praktika dumps.
# The `name` INSIDE the file must be the job name as well: the workflow report is
# updated via `Result.update_sub_result`, which matches this job's placeholder
# entry by name and silently keeps the placeholder when nothing matches - which is
# why this job showed up in the report with no status, no oracle rows and no logs,
# and why the 2026-08-13 nightly stayed green while two oracles here had failed.
JOB_META=$(python3 -c '
import sys
sys.path.insert(0, ".")
from ci.praktika._environment import _Environment
from ci.praktika.utils import Utils
name = _Environment.get().JOB_NAME
print(name)
print(Utils.normalize_string(name))
')
JOB_NAME="$(printf '%s\n' "$JOB_META" | sed -n 1p)"
NORMALIZED_JOB_NAME="$(printf '%s\n' "$JOB_META" | sed -n 2p)"
RESULT_FILE="$TMP_PATH/result_${NORMALIZED_JOB_NAME}.json"

# Same reason as in sqlancer_job.sh: the fallback result attaches whatever is in
# here, which must not be a previous local run's oracle logs.
rm -rf "$OUTPUT_PATH"
mkdir -p "$OUTPUT_PATH"

# Properly JSON-escape a string, outputting only the inner content so callers can
# embed it in "...". A failing SQLancer++ query legitimately contains backslashes
# and quotes; hand-rolled escaping produced result files that `json.loads`
# rejected, and praktika then dropped the whole job result.
json_escape() {
    printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read())[1:-1], end="")'
}

# Praktika learns what happened only from the result file, and `set -e` can abort
# the startup path below (missing jar, CREATE USER failing, server never coming
# up) long before the real result is written - which lands the job on the generic
# "no Result provided" error with none of the logs attached. Write a fallback on
# any exit until the real result replaces it.
RESULT_WRITTEN=0
write_fallback_result() {
    [ "$RESULT_WRITTEN" = "1" ] && return 0
    local files_json="" f
    for f in "$OUTPUT_PATH"/*.out "$OUTPUT_PATH"/*.err "$OUTPUT_PATH"/clickhouse-server.log*; do
        [ -f "$f" ] || continue
        case "$files_json" in *"\"$f\""*) continue ;; esac
        [ -n "$files_json" ] && files_json+=", "
        files_json+="\"$f\""
    done
    {
        printf '{\n'
        printf '  "name": "%s",\n' "$(json_escape "$JOB_NAME")"
        printf '  "status": "ERROR",\n'
        printf '  "start_time": %d,\n' "$JOB_START_TIME"
        printf '  "duration": %d,\n' "$(( $(date +%s) - JOB_START_TIME ))"
        printf '  "results": [],\n'
        printf '  "files": [%s],\n' "$files_json"
        printf '  "info": "SQLancer++ job terminated before running the oracles"\n'
        printf '}\n'
    } > "$RESULT_FILE"
}
trap write_fallback_result EXIT

if [[ -f "$CLICKHOUSE_BIN" ]]; then
    echo "$CLICKHOUSE_BIN exists"
else
    echo "$CLICKHOUSE_BIN does not exist"
    exit 1
fi

chmod +x "$CLICKHOUSE_BIN"
"$CLICKHOUSE_BIN" local --version
"$CLICKHOUSE_BIN" server -P "$PID_FILE" \
    1>"$OUTPUT_PATH/clickhouse-server.log" \
    2>"$OUTPUT_PATH/clickhouse-server.log.err" &

# Talk to the server over HTTP with `wget`, not `curl`: the shared
# `clickhouse/sqlancer-test` image (ci/docker/sqlancer-test/Dockerfile) installs
# `wget` but not `curl`, so any `curl` call dies with "command not found" and
# fails the whole job. `sqlancer_job.sh` already uses `wget` for the same reason.
# `--content-on-error` on the write queries below surfaces ClickHouse's error
# body (wget still exits non-zero on HTTP >= 400, so `set -e` fails loud).
for _ in $(seq 1 60); do
    if [[ $(wget -q -T 1 -O- 'http://localhost:8123/' 2>/dev/null) == 'Ok.' ]]; then
        break
    fi
    sleep 1
done

# Provision a SQLancer user with a real password. ClickHouse's `default` user
# has an empty password, which `com.clickhouse:clickhouse-jdbc >= 0.9.8`
# refuses to send via the JDBC URL ("Invalid query parameter value in pair
# 'password='"). Creating a dedicated user with a non-empty password is the
# least invasive workaround. Fail loud if either statement errors out -
# silently swallowing this would leave every oracle hitting an auth wall.
SQLANCER_USER="sqlancer"
SQLANCER_PASSWORD="sqlancer"
wget -q -O- --tries=1 --content-on-error --post-data="CREATE USER OR REPLACE ${SQLANCER_USER} IDENTIFIED WITH plaintext_password BY '${SQLANCER_PASSWORD}'" 'http://localhost:8123/'
# Grant everything the `default` user itself holds (CURRENT GRANTS) rather than
# `GRANT ALL`: on the embedded-config server the default user does not hold the
# full ALL set (e.g. it lacks `SHOW NAMED COLLECTIONS SECRETS`), so a plain
# `GRANT ALL ON *.* ... WITH GRANT OPTION` fails with ACCESS_DENIED (code 497)
# on current ClickHouse. CURRENT GRANTS copies exactly the default user's
# privileges, which is everything SQLancer++ needs (DDL/DML on any database).
wget -q -O- --tries=1 --content-on-error --post-data="GRANT CURRENT GRANTS ON *.* TO ${SQLANCER_USER} WITH GRANT OPTION" 'http://localhost:8123/'

cd /sqlancer-pp

JAR="$(ls target/sqlancer-*.jar | head -n 1)"
if [[ -z "$JAR" ]]; then
    echo "SQLancer++ jar not found under /sqlancer-pp/target"
    exit 1
fi

# Conservative per-oracle budget - the goal is to surface regressions, not to
# fuzz exhaustively. Four oracles * 600s = 40 min budget, well inside the
# Job.Config timeout of 3600s.
TIMEOUT=600
NUM_QUERIES=1000
NUM_THREADS=4
# All four oracles exposed by SQLancer++'s `general` provider's
# `GeneralOracleFactory`: ternary-logic partitioning on WHERE clauses, the
# non-optimising NoREC oracle, the general query-partitioning composite, and
# the random-fuzzing oracle.
ORACLES=( "WHERE" "NoREC" "QUERY_PARTITIONING" "FUZZING" )

TEST_RESULTS=()
# Per-TEST_RESULTS entry: the files attached to that oracle's row in the report
# (its own stdout/stderr), so a failing oracle's log sits next to it instead of
# only in the job-level file list.
TEST_RESULT_FILES=()
ATTACHED_FILES_ARRAY=()
# Praktika's Result.Status tokens are uppercase (OK / FAIL / ERROR); anything
# else - "success"/"failure" as this script used to write - is not `is_ok()` and
# renders as "completed but unknown" in the report.
OVERALL_STATUS=OK

for ORACLE in "${ORACLES[@]}"; do
    echo "=== Oracle: $ORACLE ==="
    error_output_file="$OUTPUT_PATH/${ORACLE}.err"
    stdout_file="$OUTPUT_PATH/${ORACLE}.out"
    ATTACHED_FILES_ARRAY+=("$error_output_file" "$stdout_file")

    if [[ $(wget -q -T 1 -O- 'http://localhost:8123/' 2>/dev/null) != 'Ok.' ]]; then
        TEST_RESULTS+=("${ORACLE},ERROR,Server is not responding")
        TEST_RESULT_FILES+=("")
        OVERALL_STATUS="FAIL"
        continue
    fi

    # SQLancer++ CLI shape: [main opts] <engine-cmd> [provider opts].
    # `general --database-engine CLICKHOUSE` selects the built-in ClickHouse
    # adapter; `--oracle` is a `general` provider option.
    set +e
    ( java -jar "$JAR" \
        --num-threads "$NUM_THREADS" \
        --num-queries "$NUM_QUERIES" \
        --timeout-seconds "$TIMEOUT" \
        --host localhost \
        --port 8123 \
        --username "$SQLANCER_USER" \
        --password "$SQLANCER_PASSWORD" \
        --print-failed false \
        --log-each-select true \
        general \
            --database-engine CLICKHOUSE \
            --oracle "$ORACLE" \
        > "$stdout_file" 2> "$error_output_file"
    )
    exit_code=$?
    set -e

    assertion_error="$(grep -i 'assert\|Exception in thread' "$error_output_file" "$stdout_file" 2>/dev/null || :)"

    if [[ $exit_code -eq 0 && -z "$assertion_error" ]]; then
        TEST_RESULTS+=("${ORACLE},OK,")
        TEST_RESULT_FILES+=("")
    else
        info="exit=${exit_code}"
        if [[ -n "$assertion_error" ]]; then
            # Collapse to one line only; JSON escaping happens at write time.
            cleaned="$(printf '%s' "$assertion_error" | tr '\n' ' ' | cut -c1-500)"
            info="${info}; ${cleaned}"
        fi
        TEST_RESULTS+=("${ORACLE},FAIL,${info}")
        TEST_RESULT_FILES+=("$stdout_file $error_output_file")
        OVERALL_STATUS="FAIL"
    fi
done

ATTACHED_FILES_ARRAY+=("$OUTPUT_PATH/clickhouse-server.log" "$OUTPUT_PATH/clickhouse-server.log.err")

# A sanitizer report or a `<Fatal>` message is a finding even when no oracle
# asserted - which is the whole point of the arm_asan_ubsan variant. Scanned
# before the server is stopped, so shutdown-time leak reports do not count.
# shellcheck source=./scripts/sqlancer_server_errors.sh
. "$REPO_DIR/ci/jobs/scripts/sqlancer_server_errors.sh"
SERVER_ERROR_REPORT="$OUTPUT_PATH/server-fatal.log"
if server_error_line="$(scan_server_errors \
        "$OUTPUT_PATH/clickhouse-server.log" "$OUTPUT_PATH/clickhouse-server.log.err" "$SERVER_ERROR_REPORT")"; then
    echo "Server log finding: $server_error_line"
    TEST_RESULTS+=("Sanitizer assert or Fatal messages in server logs,FAIL,$server_error_line")
    TEST_RESULT_FILES+=("$SERVER_ERROR_REPORT")
    OVERALL_STATUS="FAIL"
fi

# On failure, attach the per-database reproducer logs as an artifact. With
# `--log-each-select true` SQLancer++ writes every statement of each generated
# database to `logs/<dbms>/databaseN-cur.log`; the failing database's log is the
# exact CREATE/INSERT/.../SELECT sequence to reproduce the bug (the oracle's own
# "Check the *-cur.log" hint points here). Only on failure, and gzip-compressed,
# to avoid uploading a large log on clean runs.
SQLANCER_PP_LOG_DIR="/sqlancer-pp/logs"
if [[ "$OVERALL_STATUS" != "OK" && -d "$SQLANCER_PP_LOG_DIR" ]]; then
    reproducer_archive="$OUTPUT_PATH/sqlancer_pp_reproducer_logs.tar.gz"
    if tar -C "$(dirname "$SQLANCER_PP_LOG_DIR")" -czf "$reproducer_archive" "$(basename "$SQLANCER_PP_LOG_DIR")"; then
        ATTACHED_FILES_ARRAY+=("$reproducer_archive")
    fi
fi

{
    printf '{\n'
    printf '  "name": "%s",\n' "$JOB_NAME"
    printf '  "status": "%s",\n' "$OVERALL_STATUS"
    printf '  "start_time": %d,\n' "$JOB_START_TIME"
    printf '  "duration": %d,\n' "$(( $(date +%s) - JOB_START_TIME ))"
    printf '  "results": [\n'

    for i in "${!TEST_RESULTS[@]}"; do
        IFS=',' read -r test_name status info <<< "${TEST_RESULTS[i]}"
        row_files_json=""
        for f in ${TEST_RESULT_FILES[i]}; do
            [ -f "$f" ] || continue
            [ -n "$row_files_json" ] && row_files_json+=", "
            row_files_json+="\"$f\""
        done
        printf '    {"name": "%s", "status": "%s", "files": [%s], "info": "%s"}' \
            "$(json_escape "$test_name")" "$status" "$row_files_json" "$(json_escape "$info")"
        if [ "$i" -lt $((${#TEST_RESULTS[@]} - 1)) ]; then
            printf ',\n'
        else
            printf '\n'
        fi
    done

    printf '  ],\n'
    printf '  "files": ['

    # Skip files that were never created: the per-oracle logs are registered
    # before the oracle runs, so an oracle skipped after the server died leaves
    # none. Praktika replaces the whole job `info` with "WARNING: File [...] was
    # not found" for each missing path, which buries the actual result.
    files_out=""
    for f in "${ATTACHED_FILES_ARRAY[@]}"; do
        [ -f "$f" ] || continue
        [ -n "$files_out" ] && files_out+=", "
        files_out+="\"$f\""
    done
    printf '%s' "$files_out"

    printf '],\n'
    printf '  "info": ""\n'
    printf '}\n'
} > "$RESULT_FILE"
RESULT_WRITTEN=1

ls "$OUTPUT_PATH"
pkill clickhouse || true

for _ in $(seq 1 60); do
    if [[ $(wget -q -T 1 -O- 'http://localhost:8123/' 2>/dev/null) == 'Ok.' ]]; then
        sleep 1
    else
        break
    fi
done

# Praktika derives the GitHub job conclusion from this script's exit code
# (`res = run_code == 0` in `ci/praktika/runner.py`); the result file's status
# only drives the HTML report. Exiting 0 on a finding is what kept this job green
# while its report said "failure", so fail loud here.
echo "=== Summary: $OVERALL_STATUS ==="
[ "$OVERALL_STATUS" = "OK" ]
