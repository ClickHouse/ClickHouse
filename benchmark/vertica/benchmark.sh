#!/usr/bin/env bash

QUERIES_FILE="queries.sql"
TABLE=$1
TRIES=3

cat "$QUERIES_FILE" | sed "s/{table}/${TABLE}/g" | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo -n "["
    for i in $(seq 1 $TRIES); do

        RES=$((echo '\timing'; echo "$query") |
            /opt/vertica/bin/vsql -U dbadmin |
            grep -oP 'All rows formatted: [^ ]+ ms' |
            ssed -R -e 's/^All rows formatted: ([\d,]+) ms$/\1/' |
            tr ',' '.')

        [[ "$?" == "0" ]] && echo -n "$(perl -e "print ${RES} / 1000")" || echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "
    done
    echo "],"
done
