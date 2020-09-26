#!/usr/bin/env bash

#--------------------------------------------
# Description of test result:
#   Test the correctness of the optimization
#   by asserting read marks in the log.
# Relation of read marks and optimization:
#   read marks =
#       the number of monotonic marks filtered through predicates
#       + no monotonic marks count
#--------------------------------------------

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

#export CLICKHOUSE_CLIENT="clickhouse-client --send_logs_level=none"
#export CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=none
#export CURDIR=.


queries="${CURDIR}/01508_partition_pruning.queries"
while IFS= read -r sql
do
  [ -z "$sql" ] && continue
  if [[ "$sql" == select* ]] ;
  then
    CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=debug/g')
    echo "$sql"
    ${CLICKHOUSE_CLIENT} --query "$sql" 2>&1 | grep -oh "Selected .* parts by partition key, *. parts by primary key, .* marks by primary key, .* marks to read from .* ranges.*$"
    CLICKHOUSE_CLIENT=$(echo ${CLICKHOUSE_CLIENT} | sed 's/--send_logs_level=debug/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/g')
    echo ""
  else  
    ${CLICKHOUSE_CLIENT} --query "$sql"
  fi  
done < "$queries"


