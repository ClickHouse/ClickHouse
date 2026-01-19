#!/bin/bash

# DISCLAIMER:
# 1. This job script is written in bash to demonstrate "praktika CI" flexibility
# 2. Other than for demonstration purposes it would be wierd to write this in bash

set -exu

TMP_PATH=$(readlink -f ./ci/tmp/)
OUTPUT_PATH="$TMP_PATH/sqlancer_output"
PID_FILE="$TMP_PATH/clickhouse-server.pid"
CLICKHOUSE_BIN="$TMP_PATH/clickhouse"
RESULT_FILE="$TMP_PATH/result.json"
ATTACHED_FILES="["

mkdir -p $OUTPUT_PATH

if [[ -f "$CLICKHOUSE_BIN" ]]; then
    echo "$CLICKHOUSE_BIN exists"
else
    echo "$CLICKHOUSE_BIN does not exists"
    exit 1
fi

chmod +x $CLICKHOUSE_BIN
$CLICKHOUSE_BIN local --version
$CLICKHOUSE_BIN server -P $PID_FILE 1>$OUTPUT_PATH/clickhouse-server.log 2>$OUTPUT_PATH/clickhouse-server.log.err &
for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then break ; else sleep 1; fi ; done

cd /sqlancer/sqlancer-main

TIMEOUT=300
NUM_QUERIES=1000
NUM_THREADS=10
TESTS=( "TLPGroupBy" "TLPHaving" "TLPWhere" "TLPDistinct" "TLPAggregate" "NoREC" )
echo "${TESTS[@]}"

# Initialize result arrays
TEST_RESULTS=()
ATTACHED_FILES_ARRAY=()
OVERALL_STATUS=success

for TEST in "${TESTS[@]}"; do
    echo "$TEST"
    error_output_file="$OUTPUT_PATH/$TEST.err"
    ATTACHED_FILES_ARRAY+=("$error_output_file")
    
    if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]
    then
        echo "Server is OK"
        ( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads "$NUM_THREADS" --timeout-seconds "$TIMEOUT" --num-queries "$NUM_QUERIES"  --username default --password "" clickhouse --oracle "$TEST" | tee "./$TEST.out" )  3>&1 1>&2 2>&3 | tee "$error_output_file"
        assertion_error="$(grep -i assert $error_output_file ||:)"

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
    fi
done

# Add server logs to attached files
ATTACHED_FILES_ARRAY+=("$OUTPUT_PATH/clickhouse-server.log" "$OUTPUT_PATH/clickhouse-server.log.err")

# Generate JSON safely using printf
printf '{\n' > $RESULT_FILE
printf '  "name": "SQLancer",\n' >> $RESULT_FILE
printf '  "status": "%s",\n' "$OVERALL_STATUS" >> $RESULT_FILE
printf '  "start_time": null,\n' >> $RESULT_FILE
printf '  "duration": null,\n' >> $RESULT_FILE
printf '  "results": [\n' >> $RESULT_FILE

# Add test results
for i in "${!TEST_RESULTS[@]}"; do
    IFS=',' read -r test_name status info <<< "${TEST_RESULTS[i]}"
    printf '    {"name": "%s", "status": "%s", "files": [], "info": "%s"}' "$test_name" "$status" "$info" >> $RESULT_FILE
    if [ $i -lt $((${#TEST_RESULTS[@]} - 1)) ]; then
        printf ',\n' >> $RESULT_FILE
    else
        printf '\n' >> $RESULT_FILE
    fi
done

printf '  ],\n' >> $RESULT_FILE
printf '  "files": [' >> $RESULT_FILE

# Add attached files
for i in "${!ATTACHED_FILES_ARRAY[@]}"; do
    printf '"%s"' "${ATTACHED_FILES_ARRAY[i]}" >> $RESULT_FILE
    if [ $i -lt $((${#ATTACHED_FILES_ARRAY[@]} - 1)) ]; then
        printf ', ' >> $RESULT_FILE
    fi
done

printf '],\n' >> $RESULT_FILE
printf '  "info": ""\n' >> $RESULT_FILE
printf '}\n' >> $RESULT_FILE

ls "$OUTPUT_PATH"
pkill clickhouse

for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done
