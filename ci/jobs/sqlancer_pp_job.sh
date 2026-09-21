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
NORMALIZED_JOB_NAME=$(python3 -c '
import sys
sys.path.insert(0, ".")
from ci.praktika._environment import _Environment
from ci.praktika.utils import Utils
print(Utils.normalize_string(_Environment.get().JOB_NAME))
')
RESULT_FILE="$TMP_PATH/result_${NORMALIZED_JOB_NAME}.json"

mkdir -p "$OUTPUT_PATH"

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
ATTACHED_FILES_ARRAY=()
OVERALL_STATUS=success

for ORACLE in "${ORACLES[@]}"; do
    echo "=== Oracle: $ORACLE ==="
    error_output_file="$OUTPUT_PATH/${ORACLE}.err"
    stdout_file="$OUTPUT_PATH/${ORACLE}.out"
    ATTACHED_FILES_ARRAY+=("$error_output_file" "$stdout_file")

    if [[ $(wget -q -T 1 -O- 'http://localhost:8123/' 2>/dev/null) != 'Ok.' ]]; then
        TEST_RESULTS+=("${ORACLE},ERROR,Server is not responding")
        OVERALL_STATUS="failure"
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
    else
        info="exit=${exit_code}"
        if [[ -n "$assertion_error" ]]; then
            cleaned="$(printf '%s' "$assertion_error" | tr '\n' ' ' | sed 's/"/\\"/g' | cut -c1-500)"
            info="${info}; ${cleaned}"
        fi
        TEST_RESULTS+=("${ORACLE},FAIL,${info}")
        OVERALL_STATUS="failure"
    fi
done

ATTACHED_FILES_ARRAY+=("$OUTPUT_PATH/clickhouse-server.log" "$OUTPUT_PATH/clickhouse-server.log.err")

# On failure, attach the per-database reproducer logs as an artifact. With
# `--log-each-select true` SQLancer++ writes every statement of each generated
# database to `logs/<dbms>/databaseN-cur.log`; the failing database's log is the
# exact CREATE/INSERT/.../SELECT sequence to reproduce the bug (the oracle's own
# "Check the *-cur.log" hint points here). Only on failure, and gzip-compressed,
# to avoid uploading a large log on clean runs.
SQLANCER_PP_LOG_DIR="/sqlancer-pp/logs"
if [[ "$OVERALL_STATUS" != "success" && -d "$SQLANCER_PP_LOG_DIR" ]]; then
    reproducer_archive="$OUTPUT_PATH/sqlancer_pp_reproducer_logs.tar.gz"
    if tar -C "$(dirname "$SQLANCER_PP_LOG_DIR")" -czf "$reproducer_archive" "$(basename "$SQLANCER_PP_LOG_DIR")"; then
        ATTACHED_FILES_ARRAY+=("$reproducer_archive")
    fi
fi

{
    printf '{\n'
    printf '  "name": "SQLancerPP",\n'
    printf '  "status": "%s",\n' "$OVERALL_STATUS"
    printf '  "start_time": %d,\n' "$JOB_START_TIME"
    printf '  "duration": %d,\n' "$(( $(date +%s) - JOB_START_TIME ))"
    printf '  "results": [\n'

    for i in "${!TEST_RESULTS[@]}"; do
        IFS=',' read -r test_name status info <<< "${TEST_RESULTS[i]}"
        printf '    {"name": "%s", "status": "%s", "files": [], "info": "%s"}' \
            "$test_name" "$status" "$info"
        if [ "$i" -lt $((${#TEST_RESULTS[@]} - 1)) ]; then
            printf ',\n'
        else
            printf '\n'
        fi
    done

    printf '  ],\n'
    printf '  "files": ['

    for i in "${!ATTACHED_FILES_ARRAY[@]}"; do
        printf '"%s"' "${ATTACHED_FILES_ARRAY[i]}"
        if [ "$i" -lt $((${#ATTACHED_FILES_ARRAY[@]} - 1)) ]; then
            printf ', '
        fi
    done

    printf '],\n'
    printf '  "info": ""\n'
    printf '}\n'
} > "$RESULT_FILE"

ls "$OUTPUT_PATH"
pkill clickhouse || true

for _ in $(seq 1 60); do
    if [[ $(wget -q -T 1 -O- 'http://localhost:8123/' 2>/dev/null) == 'Ok.' ]]; then
        sleep 1
    else
        break
    fi
done
