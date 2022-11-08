#!/usr/bin/env bash
# script to run query to databases

function usage()
{
        cat <<EOF
usage: $0 options

This script run benhmark for database

OPTIONS:
   -c            config file where some script variables are defined
   -n            table name

   -h            Show this message
   -t            how many times execute each query. default is '3'
   -q            query file
   -e            expect file
   -s            /etc/init.d/service
   -p            table name pattern to be replaced to name. default is 'hits_10m'
EOF
}

TIMES=3
table_name_pattern=hits_10m

while getopts “c:ht:n:q:e:s:r” OPTION
do
     case $OPTION in
     c)
         source $OPTARG
         ;;
     ?)
         ;;
     esac
done

OPTIND=1

while getopts “c:ht:n:q:e:s:r” OPTION
do
     case $OPTION in
     h)
             usage
             exit 0
             ;;
         t)
         TIMES=$OPTARG
         ;;
     n)
         table_name=$OPTARG
         ;;
     q)
         test_file=$OPTARG
         ;;
     e)
         expect_file=$OPTARG
         ;;
     s)
         etc_init_d_service=$OPTARG
         ;;
     p)
         table_name_pattern=$OPTARG
         ;;
     c)
         ;;
     r)
         restart_server_each_query=1
         ;;
         ?)
             usage
             exit 0
             ;;
     esac
done

if [[ ! -f $expect_file ]]; then
    echo "Not found: expect file"
    exit 1
fi
if [[ ! -f $test_file ]]; then
    echo "Not found: test file"
    exit 1
fi

if [[ ! -f $etc_init_d_service ]]; then
    echo "Not found: /etc/init.d/service with path=$etc_init_d_service"
    use_service=0
else
    use_service=1
fi

if [[ "$table_name_pattern" == "" ]]; then
    echo "Empty table_name_pattern"
    exit 1
fi
if [[  "$table_name" == "" ]]; then
    echo "Empty table_name"
    exit 1
fi

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

        if [[  "$restart_server_each_query" == "1"  && "$use_service" == "1" ]]; then
        echo "restart server: $etc_init_d_service restart"
        sudo $etc_init_d_service restart
        fi

        for i in $(seq $TIMES)
        do
        if [[ -f $etc_init_d_service && "$use_service" == "1" ]]; then
            sudo $etc_init_d_service status
            server_status=$?
                    expect -f $expect_file ""

            if [[ "$?" != "0" || $server_status != "0" ]]; then
            echo "restart server: $etc_init_d_service restart"
            sudo $etc_init_d_service restart
            fi

            #wait until can connect to server
            restart_timer=0
            restart_limit=60
                    expect -f $expect_file "" &> /dev/null
            while [ "$?" != "0" ]; do
            echo "waiting"
            sleep 1
            let "restart_timer = $restart_timer + 1"
            if (( $restart_limit < $restart_timer )); then
                sudo $etc_init_d_service restart
                restart_timer=0
            fi
            expect -f $expect_file "" &> /dev/null
            done
        fi

        echo
        echo "times: $i"

        echo "query:" "$query"
                expect -f $expect_file "$query"

        done
    fi

    let "index = $index + 1"
    done
}

temp_test_file=temp_queries_$table_name
cat $test_file | sed s/$table_name_pattern/$table_name/g > $temp_test_file
mapfile -t test_queries < $temp_test_file

echo "start time: $(date)"
time execute "${test_queries[@]}"
echo "stop time: $(date)"
