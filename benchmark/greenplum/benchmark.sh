#!/usr/bin/env bash

filename=${1-queries.sql}
table=$2
dbname=$3
orca=${4-on}
host1=somehost
host2=somehost
mem='15GB'
cat $filename | sed "s/{table}/$table/g" | while read query ;
do
    ssh -n $host1 'echo 3 |  tee /proc/sys/vm/drop_caches; sync' > /dev/null
    ssh -n $host2 'echo 3 |  tee /proc/sys/vm/drop_caches; sync' > /dev/null
    sleep 5
    echo $query | egrep "SELECT UserID, date_trunc\('minute', EventTime\) AS m|SELECT Referer AS key, avg\(length\(Referer\)\) AS l|SELECT URL, count(1) AS c FROM.*GROUP BY URL|SELECT 1, URL, count\(1\) AS c FROM.*GROUP BY 1" && mem='10GB'
   echo $query | egrep 'SELECT DISTINCT|GROUP BY UserID, SearchPhrase LIMIT 10|count\(DISTINCT UserID\) AS u' && mem='5GB'
    echo "####################"
    echo "$query"
    echo "Timestamp_begin:$(date)"
    echo  "\\timing off \\\\set optimizer=$orca; set effective_cache_size='256MB'; set statement_mem='$mem';\\timing on \\\\ $query;"  | psql -p 5432 -h 'localhost' -o /dev/null -U gpadmin ${dbname}
    echo "Timestamp_end:$(date)"
    echo "Timestamp_begin:$(date)"
    echo  "\\timing off \\\\set optimizer=$orca; set effective_cache_size='50GB'; set statement_mem='$mem';\\timing on \\\\ $query;"  | psql -p 5432 -h 'localhost' -o /dev/null -U gpadmin ${dbname}
    echo "Timestamp_end:$(date)"
    echo "Timestamp_begin:$(date)"
    echo  "\\timing off \\\\set optimizer=$orca; set effective_cache_size='50GB'; set statement_mem='$mem';\\timing on \\\\ $query;"  | psql -p 5432 -h 'localhost' -o /dev/null -U gpadmin ${dbname}
    echo "Timestamp_end:$(date)"
    echo "$query"
    echo '####################'
done
