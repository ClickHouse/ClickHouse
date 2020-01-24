#!/usr/bin/env bash

QUERIES_FILE="queries.sql"
TABLE=$1
TRIES=3

cat "$QUERIES_FILE" | sed "s|{table}|\"${TABLE}\"|g" | while read query; do

    echo -n "["
    for i in $(seq 1 $TRIES); do
        while true; do
            RES=$(command time -f %e -o /dev/stdout curl -sS --location-trusted -H "Authorization: OAuth $YT_TOKEN" "$YT_PROXY.yt.yandex.net/query?default_format=Null&database=*$YT_CLIQUE_ID" --data-binary @- <<< "$query" 2>/dev/null) && break;
        done

        [[ "$?" == "0" ]] && echo -n "${RES}" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done
