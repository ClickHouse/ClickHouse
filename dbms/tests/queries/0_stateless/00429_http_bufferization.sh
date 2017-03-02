#!/bin/bash
set -e

max_block_size=10
URL='http://localhost:8123/'

function query {
	echo "SELECT toUInt8(intHash64(number)) FROM system.numbers LIMIT $1 FORMAT RowBinary"
	#echo "SELECT toUInt8(number) FROM system.numbers LIMIT $1 FORMAT RowBinary"
}

function ch_url() {
	curl -sS "$URL?max_block_size=$max_block_size&$1" -d "`query $2`"
}


# Check correct exceptions handling

exception_pattern="Code: 307, e\.displayText() = DB::Exception:[[:print:]]* e\.what() = DB::Exception$"

function check_only_exception() {
	local res=`ch_url "$1" "$2"`
	#(echo "$res")
	#(echo "$res" | wc -l)
	#(echo "$res" | grep -c "^$exception_pattern")
	[[ `echo "$res" | wc -l` -eq 1 ]] && echo OK || echo FAIL
	[[ $(echo "$res" | grep -c "^$exception_pattern") -eq 1 ]] && echo OK || echo FAIL
}

function check_last_line_exception() {
	local res=`ch_url "$1" "$2"`
	echo "$res" > res
	#echo "$res" | wc -c
	#echo "$res" | tail -n -2
	[[ $(echo "$res" | tail -n -1 | grep -c "$exception_pattern") -eq 1 ]] && echo OK || echo FAIL
	[[ $(echo "$res" | head -n -1 | grep -c "$exception_pattern") -eq 0 ]] && echo OK || echo FAIL
}

function check_exception_handling() {
check_only_exception "max_result_bytes=1000" 						1001
check_only_exception "max_result_bytes=1000&wait_end_of_query=1"	1001
echo
check_only_exception "max_result_bytes=1048576&buffer_size=1048576&wait_end_of_query=0" 1048577
check_only_exception "max_result_bytes=1048576&buffer_size=1048576&wait_end_of_query=1" 1048577
echo
check_only_exception "max_result_bytes=1500000&buffer_size=2500000&wait_end_of_query=0" 1500001
check_only_exception "max_result_bytes=1500000&buffer_size=1500000&wait_end_of_query=1" 1500001
echo
check_only_exception 		"max_result_bytes=4000000&buffer_size=2000000&wait_end_of_query=1" 5000000
check_only_exception 		"max_result_bytes=4000000&wait_end_of_query=1" 5000000
check_last_line_exception 	"max_result_bytes=4000000&buffer_size=2000000&wait_end_of_query=0" 5000000
}

#check_exception_handling


max_block_size=500000
corner_sizes="1048576 `seq 500000 1000000 3500000`"
# Check HTTP results with clickhouse-client in normal case

function cmp_cli_and_http() {
	clickhouse-client -q "`query $1`" > res1
	ch_url "buffer_size=$2&wait_end_of_query=0" "$1" > res2
	ch_url "buffer_size=$2&wait_end_of_query=1" "$1" > res3
	cmp res1 res2
	cmp res1 res3
	rm -rf res1 res2 res3
}

function check_cli_and_http() {
	for input_size in $corner_sizes; do
		for buffer_size in $corner_sizes; do
			#echo "$input_size" "$buffer_size"
			cmp_cli_and_http "$input_size" "$buffer_size"
		done
	done
}

check_cli_and_http

# Check HTTP internal compression in normal case (clickhouse-compressor required)

function cmp_http_compression() {
	clickhouse-client -q "`query $1`" > res0
	ch_url 'compress=1' $1 | clickhouse-compressor --decompress > res1
	ch_url "compress=1&buffer_size=$2&wait_end_of_query=0" $1 | clickhouse-compressor --decompress > res2
	ch_url "compress=1&buffer_size=$2&wait_end_of_query=1" $1 | clickhouse-compressor --decompress > res3
	cmp res0 res1
	cmp res1 res2
	cmp res1 res3
	rm -rf res0 res1 res2 res3
}

function check_http_compression() {
	for input_size in $corner_sizes; do
		for buffer_size in $corner_sizes; do
			#echo "$input_size" "$buffer_size"
			cmp_http_compression "$input_size" "$buffer_size"
		done
	done
}

has_compressor=$(command -v clickhouse-compressor &>/dev/null && echo 1)
[[ has_compressor ]] && check_http_compression || true
