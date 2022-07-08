#!/bin/bash

TRIES=3
cat queries.sql | while read query; do
    sync
    for i in $(seq 1 100); do
        CHECK=$(curl -o /dev/null -w '%{http_code}' -s -XPOST -H'Content-Type: application/json' http://localhost:8888/druid/v2/sql/ -d @check.json })
	if [[ "$CHECK" == "200" ]]; then
	    break
	fi
	sleep 1
    done
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
    echo -n "["
    for i in $(seq 1 $TRIES); do
        echo "{\"query\":\"$query\", \"context\": {\"timeout\": 1000000} }"| sed -e 's EventTime __time g' | tr -d ';' > query.json
	curl -o /dev/null -w '%{http_code} %{time_total}\n' -s -XPOST -H'Content-Type: application/json' http://localhost:8888/druid/v2/sql/ -d @query.json | awk '{ if($1=="200") printf $2; else printf "null"; }'
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
    # Ugly hack to measure independently queries. Otherwise some queries make Druid degraded and results are incorrect. For example after Q13 even SELECT 1 works for 7 seconds
    pkill -f historical
    sleep 3
done
