#!/usr/bin/env bash                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                              
CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)                                                                                                                                                                                                          
# shellcheck source=../shell_config.sh                                                                                                                                                                                                                        
. "$CURDIR"/../shell_config.sh   

python3 -c "print('{{\"a\":\"{}\", \"b\":\"{}\"}}'.format('clickhouse'* 10000000, 'dbms' * 100000000))" > big_json.json

${CLICKHOUSE_LOCAL} --input_format_parallel_parsing=1 --max_memory_usage=0 -q "select count() from file('big_json.json', 'JSONEachRow', 'a String, b String')" 2>&1 | grep -q "min_chunk_bytes_for_parallel_parsing" && echo "Ok." || echo "FAIL" ||: