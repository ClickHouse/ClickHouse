#!/bin/bash
# script to run query to databases
if [[ "$#" != "2" &&  "$#" != "3" ]]; then 
    echo "script to run request for database."
    echo "usage: query_file  expect_file [etc_init_d_service]"
    exit 1
fi

test_file=$1
expect_file=$2
etc_init_d_service=$3

TIMES=3

function execute()
{
    queries=("${@}")
    queries_count=${#queries[@]}
    
    if [ -z $TIMES ]; then
	TIMES=1
    fi
    
    index=0
    while [ "$index" -lt "$queries_count" ]; do
	query=${queries[$index]}

	if [[ $query == "" ]]; then
	    let "index = $index + 1"
	    continue
	fi
	
   	comment_re='--.*'
	if [[ $query =~ $comment_re ]]; then
	    echo "$query"
	    echo
	else	    
	    sync
	    sudo sh -c "echo 3 > /proc/sys/vm/drop_caches"

	    for i in $(seq $TIMES)
	    do
		echo
		echo "times: $i"  
		
		echo "query:" "$query"
                expect -f $expect_file "$query"		

		if [ -e $etc_init_d_service ]; then
		    sudo $etc_init_d_service status
		    if [ "$?" != "0" ]; then
			sudo $etc_init_d_service restart
		    fi
		fi
	    done
	fi

	let "index = $index + 1"
    done
}

mapfile -t test_queries < $test_file

echo "start time: $(date)"
time execute "${test_queries[@]}"
echo "stop time: $(date)"
