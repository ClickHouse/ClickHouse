#!/usr/bin/env bash
#set -e

[[ -n "$1" ]] && host="$1" || host="localhost"
[[ -n "$2" ]] && min_timestamp="$2" || min_timestamp=$(( $(date +%s) - 60 ))
[[ -n "$3" ]] && max_timestamp="$3" || max_timestamp=$(( $(date +%s) + 60 ))
[[ -n "$4" ]] && client="$4" || client="clickhouse-client"

timestamps=`seq $min_timestamp $max_timestamp`

function reliable_insert {
    local ts="$1"
    num_tries=0
    while true; do
        if (( $num_tries > 20 )); then
            echo "Too many retries" 1>&2
            exit -1
        fi

        #echo clickhouse-client --host $host -q "INSERT INTO simple VALUES (0, $ts, '$ts')"
        res=`$client --host $host -q "INSERT INTO simple VALUES (0, $ts, '$ts')" 2>&1`
        rt=$?
        num_tries=$(($num_tries+1))

        if (( $rt == 0 )); then break; fi
        if [[ $res == *"Code: 319. "*"Unknown status, client must retry"* || $res == *"Code: 999. "* ]]; then
            continue
        else
            echo FAIL "$res" 1>&2
            exit -1
        fi
    done;
}

for i in $timestamps; do

    cur_timestamp=$(date +%s)
    while (( $cur_timestamp < $i )); do
        ts=`shuf -i $min_timestamp-$cur_timestamp -n 1`
        reliable_insert "$ts"
        cur_timestamp=$(date +%s)
    done

    reliable_insert "$i"
done
sleep 1
