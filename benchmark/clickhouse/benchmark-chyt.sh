#!/usr/bin/env bash

QUERIES_FILE="queries.sql"
TABLE=$1
TRIES=3

cat "$QUERIES_FILE" | sed "s|{table}|\"${TABLE}\"|g" | while read query; do

    echo -n "["
    for i in $(seq 1 $TRIES); do
        while true; do
            RES=$(command time -f %e -o /dev/stdout curl -sS -G --data-urlencode "query=$query" --data "default_format=Null&max_memory_usage=100000000000&max_memory_usage_for_all_queries=100000000000&max_concurrent_queries_for_user=100&database=*$YT_CLIQUE_ID" --location-trusted -H "Authorization: OAuth $YT_TOKEN" "$YT_PROXY.yt.yandex.net/query" 2>/dev/null);
            if [[ $? == 0 ]]; then
                [[ $RES =~ 'fail|Exception' ]] || break;
            fi
        done

        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done
