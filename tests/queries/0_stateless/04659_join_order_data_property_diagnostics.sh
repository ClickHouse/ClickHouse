#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cleanup() {
	$CLICKHOUSE_CLIENT --multiquery --query "
        DROP TABLE IF EXISTS data_properties_first;
        DROP TABLE IF EXISTS data_properties_second;
        DROP TABLE IF EXISTS data_properties_third;
    " >/dev/null 2>&1
}
trap cleanup EXIT
cleanup

$CLICKHOUSE_CLIENT --multiquery --query "
    CREATE TABLE data_properties_first (first_id UInt64)
    ENGINE = MergeTree ORDER BY first_id SETTINGS auto_statistics_types = '';
    CREATE TABLE data_properties_second (second_first_id UInt64)
    ENGINE = MergeTree ORDER BY second_first_id SETTINGS auto_statistics_types = '';
    CREATE TABLE data_properties_third (third_first_id UInt64)
    ENGINE = MergeTree ORDER BY third_first_id SETTINGS auto_statistics_types = '';
"

QUERY="
SELECT trimLeft(explain)
FROM
(
    EXPLAIN keep_logical_steps = 1, actions = 1
    SELECT *
    FROM (SELECT first_id FROM data_properties_first GROUP BY first_id) AS first
    INNER JOIN (SELECT second_first_id FROM data_properties_second GROUP BY second_first_id) AS second
        ON first_id = second_first_id
    INNER JOIN data_properties_third ON first_id = third_first_id
)
WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%'
"

FINAL_FACT_QUERY="
EXPLAIN
SELECT *
FROM (SELECT first_id FROM data_properties_first GROUP BY first_id) AS first
INNER JOIN (SELECT second_first_id FROM data_properties_second GROUP BY second_first_id) AS second
    ON first_id = second_first_id
"

run_query() {
	local use_proven_uniqueness=$1
	local diagnostics=$2
	local logs_level=$3
	$CLICKHOUSE_CLIENT \
		--allow_repeated_settings \
		--send_logs_level="$logs_level" \
		--explain_query_plan_default=legacy \
		--enable_analyzer=1 \
		--enable_parallel_replicas=0 \
		--enable_join_runtime_filters=0 \
		--enable_join_transitive_predicates=0 \
		--query_plan_optimize_join_order_limit=10 \
		--query_plan_optimize_join_order_algorithm=greedy \
		--query_plan_optimize_join_order_randomize=42 \
		--query_plan_join_swap_table=0 \
		--query_plan_optimize_join_order_use_proven_uniqueness="$use_proven_uniqueness" \
		--query_plan_optimize_join_order_data_property_diagnostics="$diagnostics" \
		--query "$QUERY"
}

run_final_fact_query() {
	$CLICKHOUSE_CLIENT \
		--send_logs_level=trace \
		--explain_query_plan_default=legacy \
		--enable_analyzer=1 \
		--enable_parallel_replicas=0 \
		--enable_join_runtime_filters=0 \
		--enable_join_transitive_predicates=0 \
		--query_plan_optimize_join_order_limit=10 \
		--query_plan_optimize_join_order_algorithm=greedy \
		--query_plan_join_swap_table=0 \
		--query_plan_optimize_join_order_use_proven_uniqueness=1 \
		--query_plan_optimize_join_order_data_property_diagnostics=1 \
		--query "$FINAL_FACT_QUERY"
}

filtered_plan() {
	run_query "$1" "$2" none 2>/dev/null |
		grep -E '^[[:space:]]*(Join:|ResultRows:)' |
		sed -E 's/^[[:space:]]+//'
}

has_property_diagnostics() {
	grep -Eq 'data properties:|Canonical join-order data properties:|Canonical join-order cap assessments:'
}

has_final_property_diagnostics() {
	grep -Fq 'Canonical join-order data properties:'
}

has_cap_assessment_diagnostics() {
	grep -Fq 'Canonical join-order cap assessments:'
}

check_trace_case() {
	local label=$1
	local use_proven_uniqueness=$2
	local diagnostics=$3
	local expect_properties=$4
	local output
	local properties_present
	output=$(run_query "$use_proven_uniqueness" "$diagnostics" trace 2>&1)

	if grep -q 'Optimized join order in' <<<"$output"; then
		echo "$label ordinary trace: OK"
	else
		echo "$label ordinary trace: FAIL"
	fi

	if has_property_diagnostics <<<"$output"; then
		properties_present=1
	else
		properties_present=0
	fi
	if [[ "$expect_properties" == 1 ]]; then
		if [[ "$properties_present" == 1 ]] \
			&& has_final_property_diagnostics <<<"$output" \
			&& has_cap_assessment_diagnostics <<<"$output"; then
			echo "$label property diagnostics: OK"
		else
			echo "$label property diagnostics: FAIL"
		fi
	elif [[ "$properties_present" == 0 ]]; then
		echo "$label property diagnostics: OK"
	else
		echo "$label property diagnostics: FAIL"
	fi
}

check_trace_case "both settings off" 0 0 0
check_trace_case "unique keys only" 1 0 0
check_trace_case "diagnostics only" 0 1 1
check_trace_case "both settings on" 1 1 1

legacy_plan=$(filtered_plan 0 0)
diagnostics_plan=$(filtered_plan 0 1)
if [[ "$legacy_plan" == "$diagnostics_plan" ]]; then
	echo "legacy plan diagnostics parity: OK"
else
	echo "legacy plan diagnostics parity: FAIL"
fi

capped_plan=$(filtered_plan 1 0)
capped_diagnostics_plan=$(filtered_plan 1 1)
if [[ "$capped_plan" == "$capped_diagnostics_plan" ]]; then
	echo "capped plan diagnostics parity: OK"
else
	echo "capped plan diagnostics parity: FAIL"
fi

first_capped_result=$(grep '^ResultRows:' <<<"$capped_plan" | head -n 1)
if [[ "$first_capped_result" == 'ResultRows: ~~4012482' ]]; then
	echo "unique-key capped estimate: OK"
else
	echo "unique-key capped estimate: FAIL"
fi

final_fact_line=$(run_final_fact_query 2>&1 | grep -F 'Canonical join-order data properties:' | tail -n 1)
if grep -Fq 'region=' <<<"$final_fact_line" \
	&& grep -Fq 'subset=[' <<<"$final_fact_line" \
	&& grep -Fq 'predicate_closure=' <<<"$final_fact_line" \
	&& grep -Fq 'output_contract=' <<<"$final_fact_line" \
	&& grep -Fq 'groups=' <<<"$final_fact_line" \
	&& ! grep -Fq 'Selected join-order data properties:' <<<"$final_fact_line"; then
	echo "canonical group diagnostics: OK"
else
	echo "canonical group diagnostics: FAIL"
fi
