#!/bin/bash

# DISCLAIMER:
# 1. This job script is written in bash to demonstrate "praktika CI" flexibility
# 2. Other than for demonstration purposes it would be wierd to write this in bash

set -exu

TMP_PATH=$(readlink -f ./ci/tmp/)
OUTPUT_PATH="$TMP_PATH/sqlancer_output"
PID_FILE="$TMP_PATH/clickhouse-server.pid"
CLICKHOUSE_BIN="$TMP_PATH/clickhouse"
RESULT_FILE="$TMP_PATH/result_job.json"

mkdir -p $OUTPUT_PATH

# Write result on any exit to ensure logs are always uploaded as artifacts
write_result() {
    local status="${OVERALL_STATUS:-error}"
    local info="${RESULT_INFO:-Script terminated unexpectedly}"

    # Build results array
    local results_json=""
    if [ ${#TEST_RESULTS[@]} -gt 0 ]; then
        for i in "${!TEST_RESULTS[@]}"; do
            IFS=',' read -r test_name test_status test_info <<< "${TEST_RESULTS[i]}"
            if [ -n "$results_json" ]; then
                results_json+=","
            fi
            results_json+=$(printf '\n    {"name": "%s", "status": "%s", "files": [], "info": "%s"}' "$test_name" "$test_status" "$test_info")
        done
    fi

    # Build files array
    local files_json=""
    for f in "$OUTPUT_PATH/clickhouse-server.log" "$OUTPUT_PATH/clickhouse-server.log.err"; do
        if [ -f "$f" ]; then
            if [ -n "$files_json" ]; then
                files_json+=", "
            fi
            files_json+="\"$f\""
        fi
    done
    for f in "${ATTACHED_FILES_ARRAY[@]+"${ATTACHED_FILES_ARRAY[@]}"}"; do
        if [ -f "$f" ]; then
            if [ -n "$files_json" ]; then
                files_json+=", "
            fi
            files_json+="\"$f\""
        fi
    done

    printf '{\n' > $RESULT_FILE
    printf '  "name": "SQLancer",\n' >> $RESULT_FILE
    printf '  "status": "%s",\n' "$status" >> $RESULT_FILE
    printf '  "start_time": null,\n' >> $RESULT_FILE
    printf '  "duration": null,\n' >> $RESULT_FILE
    printf '  "results": [%s\n  ],\n' "$results_json" >> $RESULT_FILE
    printf '  "files": [%s],\n' "$files_json" >> $RESULT_FILE
    printf '  "info": "%s"\n' "$info" >> $RESULT_FILE
    printf '}\n' >> $RESULT_FILE
}

# Initialize variables used by the trap handler
TEST_RESULTS=()
ATTACHED_FILES_ARRAY=()
OVERALL_STATUS="error"
RESULT_INFO=""

trap write_result EXIT

if [[ -f "$CLICKHOUSE_BIN" ]]; then
    echo "$CLICKHOUSE_BIN exists"
else
    echo "$CLICKHOUSE_BIN does not exists"
    exit 1
fi

chmod +x $CLICKHOUSE_BIN
$CLICKHOUSE_BIN local --version

echo "Starting ClickHouse server..."
$CLICKHOUSE_BIN server -P $PID_FILE 1>$OUTPUT_PATH/clickhouse-server.log 2>$OUTPUT_PATH/clickhouse-server.log.err &
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-main

TIMEOUT=300
NUM_QUERIES=1000
NUM_THREADS=10
TESTS=( "TLPGroupBy" "TLPHaving" "TLPWhere" "TLPDistinct" "TLPAggregate" "NoREC" )
echo "${TESTS[@]}"

OVERALL_STATUS=success

for TEST in "${TESTS[@]}"; do
    echo "$TEST"
    error_output_file="$OUTPUT_PATH/$TEST.err"
    ATTACHED_FILES_ARRAY+=("$error_output_file")

    if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then
        echo "Server is OK"
        ( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads "$NUM_THREADS" --timeout-seconds "$TIMEOUT" --num-queries "$NUM_QUERIES"  --username default --password "" clickhouse --oracle "$TEST" | tee "./$TEST.out" )  3>&1 1>&2 2>&3 | tee "$error_output_file"

        if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) != 'Ok.' ]]; then
            echo "Server crashed during $TEST"
            TEST_RESULTS+=("${TEST},ERROR,Server crashed during test")
            OVERALL_STATUS="failure"
            RESULT_INFO="Server crashed during $TEST"
            break
        fi

        assertion_error="$(grep 'AssertionError' "$error_output_file" ||:)"

        if [ -z "$assertion_error" ]; then
            TEST_RESULTS+=("${TEST},OK,")
        else
            # Escape for JSON: replace newlines with spaces and escape quotes
            assertion_error_clean="$(printf '%s' "$assertion_error" | tr '\n' ' ' | sed 's/"/\\"/g')"
            TEST_RESULTS+=("${TEST},FAIL,${assertion_error_clean}")
            OVERALL_STATUS="failure"
        fi
    else
        TEST_RESULTS+=("${TEST},ERROR,Server is not responding")
        OVERALL_STATUS="failure"
        RESULT_INFO="Server is not responding before $TEST"
        break
    fi
done

ls "$OUTPUT_PATH"

pkill clickhouse || true
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O- 2>/dev/null) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done
