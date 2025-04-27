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

# write Result json file for praktika ci.
cat <<EOF > $RESULT_FILE
{
  "name": "SQLancer",
  "status": "OVERALL_STATUS",
  "start_time": null,
  "duration": null,
  "results": [
    TEST_CASE_RESULT
  ],
  "files": ATTACHED_FILES,
  "info": ""
}
EOF

TEST_CASE_RESULT_PATTERN="{\"name\": \"NAME\", \"status\": \"FAIL\",  \"files\": [], \"info\": \"INFO\"}"
OVERALL_STATUS=success

for TEST in "${TESTS[@]}"; do
    echo "$TEST"
    test_case_result="$(echo $TEST_CASE_RESULT_PATTERN | sed s/NAME/$TEST/)"
    error_output_file="$OUTPUT_PATH/$TEST.err"
    if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]
    then
        echo "Server is OK"
        ( java -jar target/sqlancer-*.jar --log-each-select true --print-failed false --num-threads "$NUM_THREADS" --timeout-seconds "$TIMEOUT" --num-queries "$NUM_QUERIES"  --username default --password "" clickhouse --oracle "$TEST" | tee "./$TEST.out" )  3>&1 1>&2 2>&3 | tee "$error_output_file"
        assertion_error="$(grep -i assert $error_output_file ||:)"

        if [ -z "$assertion_error" ]; then
          test_case_result="$(echo "$test_case_result" | sed s/FAIL/OK/)"
          test_case_result="$(echo "$test_case_result" | sed s/INFO//)"
        else
          test_case_result="$(echo "$test_case_result" | sed s/INFO/$assertion_error/)"
          OVERALL_STATUS="failure"
        fi

    else
        test_case_result="$(echo "$test_case_result" | sed s/FAIL/ERROR/)"
        test_case_result="$(echo "$test_case_result" | sed s/INFO/Server is not responding/)"
        OVERALL_STATUS="failure"
    fi
    sed -i "s/TEST_CASE_RESULT/$test_case_result\n,TEST_CASE_RESULT/" $RESULT_FILE
    ATTACHED_FILES="$ATTACHED_FILES \"${error_output_file}\","
done

ATTACHED_FILES="$ATTACHED_FILES \"$OUTPUT_PATH/clickhouse-server.log\", \"$OUTPUT_PATH/clickhouse-server.log.err\"]"

sed -i "s/OVERALL_STATUS/$OVERALL_STATUS/" $RESULT_FILE
sed -i "s/,TEST_CASE_RESULT//" $RESULT_FILE
sed -i "s/TEST_CASE_RESULT//" $RESULT_FILE
sed -i "s|ATTACHED_FILES|$ATTACHED_FILES|" $RESULT_FILE

ls "$OUTPUT_PATH"
pkill clickhouse

for _ in $(seq 1 60); do if [[ $(wget -q 'localhost:8123' -O-) == 'Ok.' ]]; then sleep 1 ; else break; fi ; done
