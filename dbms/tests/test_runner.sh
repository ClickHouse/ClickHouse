#!/bin/bash
# script to run query to databases

function usage()
{
        cat <<EOF   
usage: $0 test_queries.sql

This script run queries from test file and check output

OPTIONS:
   -h            Show this message
EOF
}

TIMES=1
test_file=$1
log=$test_file"_log_$(date +%H_%M_%d_%m_%Y)"
echo "Log to $log"

while getopts “h” OPTION
do
     case $OPTION in
	 h)
             usage
             exit 0
             ;;
         ?)
             usage
             exit 0
             ;;
     esac
done

if [[ ! -f $test_file ]]; then
    echo "Not found: test file"
    exit 1
fi

function execute()
{
    echo "start time: $(date)" > $log
    queries=("${@}")
    queries_count=${#queries[@]}
        
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
	    echo "query:" "$query"
            expect -c "#!/bin/bash
#!/bin/expect

# Set timeout
set timeout 600

# Get arguments
set query [lindex $argv 0]

spawn clickhouse-client --multiline;
expect \":) \"
send \"$query;\r\";
expect \":) \"
send \"quit\";" >> "$log"
	fi
	let "index = $index + 1"
    done

    echo "stop time: $(date)" >> $log
}

mapfile -t test_queries < $test_file

execute "${test_queries[@]}"

echo "Error list"
cat $log

echo 
echo Error list\:
cat $log | grep -iP 'error|exception'
