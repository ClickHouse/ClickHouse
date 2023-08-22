#!/bin/bash
ckhost="localhost"
ckport=("9000" "9001" "9002" "9003")
WORKING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
OUTPUT_DIR="${WORKING_DIR}/output"
LOG_DIR="${OUTPUT_DIR}/log"
RAWDATA_DIR="${WORKING_DIR}/rawdata_dir"
DATABASE_DIR="${WORKING_DIR}/database_dir"
CLIENT_SCRIPTS_DIR="${WORKING_DIR}/client_scripts"
LOG_PACK_FILE="$(date +%Y-%m-%d-%H-%M-%S)"
QUERY_FILE="queries_ontime.sql"
SERVER_BIND_CMD[0]="TBD" #numactl -C 0-29,120-149
SERVER_BIND_CMD[1]="TBD" #numactl -C 30-59,150-179
SERVER_BIND_CMD[2]="TBD" #numactl -m 1 -N 1
SERVER_BIND_CMD[3]="TBD" #numactl -m 1 -N 1
CLIENT_BIND_CMD="numactl -m 1 -N 1"
SSB_GEN_FACTOR=20
TABLE_NAME="ontime"
TALBE_ROWS="183953732"
CODEC_CONFIG="lz4 deflate zstd"

# define instance number
inst_num=$1
if [ ! -n "$1" ]; then
        echo "Please clarify instance number from 1,2,3 or 4"
        exit 1
else
        echo "Benchmarking with instance number:$1"
        for i in $(seq 0 $[inst_num-1])
        do
                if [ "${SERVER_BIND_CMD[i]}" = "TBD" ];then
                        echo "Please customize SERVER_BIND_CMD[${i}] in allin1_ssb.sh.
The cores of one socket need to be divided equally and assigned to the server.
For example: 'numactl -C 0-29,120-149' or 'numactl -C 30-59,150-179'"
                        exit 1
                fi
        done
fi

if [ ! -d "$OUTPUT_DIR" ]; then
mkdir $OUTPUT_DIR
fi
if [ ! -d "$LOG_DIR" ]; then
mkdir $LOG_DIR
fi
if [ ! -d "$RAWDATA_DIR" ]; then
mkdir $RAWDATA_DIR
fi

# define different directories
dir_server=("" "_s2" "_s3" "_s4")
ckreadSql=""
supplier_table=""
lineorder_table=""
customer_table=""

lineorder_flat_table=""
 
function insert_data(){
                exit 0
}

function check_sql(){
        select_sql="select * from "$1" limit 1"
        clickhouse client --host ${ckhost} --port $2 --multiquery -q"${select_sql}"
}

function check_table(){
        checknum=0
        source_tables="customer part supplier lineorder lineorder_flat"
        test_tables=${1:-${source_tables}}
        echo "Checking table data required in server..."
        for i in $(seq 0 $[inst_num-1])
        do
                for j in `echo ${test_tables}`
                do
                        check_sql $j ${ckport[i]} &> /dev/null || {
                                let checknum+=1 && insert_data "$j" ${ckport[i]}
                        }
                done
        done

        for i in $(seq 0 $[inst_num-1])
        do
                echo "clickhouse client --host ${ckhost} --port ${ckport[i]} -m -q\"select count() from ${TABLE_NAME};\""
                var=$(clickhouse client --host ${ckhost} --port ${ckport[i]} -m -q"select count() from ${TABLE_NAME};")
                if [ $var -eq $TALBE_ROWS ];then
                        echo "Instance_${i} Table data integrity check OK -> Rows:$var"
                else
                        echo  "Instance_${i} Table data integrity check Failed -> Rows:$var"
                        kill_instance_and_exit
                fi
        done
        if [ $checknum -gt 0 ];then
                echo "Need sleep 10s after first table data insertion...$checknum"
                sleep 10
        fi
}

function check_instance(){
instance_alive=0
for i in {1..10}
do
        sleep 1
        netstat -nltp | grep ${1} > /dev/null
        if [ $? -ne 1 ];then
                instance_alive=1
                break
        fi
        
done

if [ $instance_alive -eq 0 ];then
        echo "check_instance -> clickhouse server instance faild to launch due to 10s timeout!"
        exit 1
else
        echo "check_instance -> clickhouse server instance launch successfully!"
fi
}

function start_clickhouse_for_insertion(){
        echo "start_clickhouse_for_insertion"
        for i in $(seq 0 $[inst_num-1])
	do                
                echo "cd ${DATABASE_DIR}/$1${dir_server[i]}"
                echo "${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&${LOG_DIR}/${1}_${i}_server_log& > /dev/null"
                
	        cd ${DATABASE_DIR}/$1${dir_server[i]}
	        ${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&${LOG_DIR}/${1}_${i}_server_log& > /dev/null
                check_instance ${ckport[i]}
        done
}

function start_clickhouse_for_stressing(){
        echo "start_clickhouse_for_stressing"
        for i in $(seq 0 $[inst_num-1])
	do
                echo "cd ${DATABASE_DIR}/$1${dir_server[i]}"
                echo "${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&/dev/null&"
                
	        cd ${DATABASE_DIR}/$1${dir_server[i]}
	        ${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&/dev/null&
                check_instance ${ckport[i]}
        done
}
yum -y install git make gcc sudo net-tools &> /dev/null
pip3 install clickhouse_driver numpy &> /dev/null


function kill_instance(){
instance_alive=1  
for i in {1..2}
do
	pkill clickhouse && sleep 5
        instance_alive=0        
        for i in $(seq 0 $[inst_num-1])
        do
                netstat -nltp | grep ${ckport[i]} > /dev/null
                if [ $? -ne 1 ];then
                        instance_alive=1
                        break;
                fi
        done
        if [ $instance_alive -eq 0 ];then
                break;
        fi        
done
if [ $instance_alive -eq 0 ];then
        echo "kill_instance OK!"
else
        echo "kill_instance Failed -> clickhouse server instance still alive due to 10s timeout"
        exit 1        
fi
}

function run_test(){
is_xml=0
for i in $(seq 0 $[inst_num-1])
do
        if [ -f ${DATABASE_DIR}/${1}${dir_server[i]}/config_${1}${dir_server[i]}.xml ]; then
                is_xml=$[is_xml+1]
        fi
done
if [ $is_xml -eq $inst_num ];then
        echo "Benchmark with $inst_num instance"
        kill_instance
        start_clickhouse_for_insertion ${1}

        for i in $(seq 0 $[inst_num-1])
        do
                clickhouse client --host ${ckhost} --port ${ckport[i]} -m -q"show databases;" >/dev/null
        done

        if [ $? -eq 0 ];then
                check_table
        fi

        if [ $1 == "deflate" ];then
	        test -f ${LOG_DIR}/${1}_server_log && deflatemsg=`cat ${LOG_DIR}/${1}_server_log | grep DeflateJobHWPool`
	        if [ -n "$deflatemsg" ];then
	                echo ------------------------------------------------------
	                echo $deflatemsg
	                echo ------------------------------------------------------
	        fi
	fi
        echo "Check table data required in server_${1} -> Done! "
        
        kill_instance
        start_clickhouse_for_stressing ${1}
        for i in $(seq 0 $[inst_num-1])
        do
                clickhouse client --host ${ckhost} --port ${ckport[i]} -m -q"show databases;" >/dev/null
        done
        if [ $? -eq 0 ];then
                test -d ${CLIENT_SCRIPTS_DIR}  && cd ${CLIENT_SCRIPTS_DIR}
                echo "Client stressing... "
                echo "${CLIENT_BIND_CMD} python3 client_stressing_test.py ${QUERY_FILE} $inst_num &> ${LOG_DIR}/${1}.log"
                ${CLIENT_BIND_CMD} python3 client_stressing_test.py ${QUERY_FILE} $inst_num &> ${LOG_DIR}/${1}.log
                echo "Completed client stressing, checking log... "
                finish_log=`grep "Finished" ${LOG_DIR}/${1}.log | wc -l`
	        if [ $finish_log -eq 1 ] ;then
	                test -f ${LOG_DIR}/${1}.log && echo  "${1}.log ===> ${LOG_DIR}/${1}.log"
                        kill_instance
	        else
	                echo "No find 'Finished' in client log -> Performance test may fail"
                        kill_instance_and_exit
	        fi
	    else
                echo "${1} clickhouse server start fail"
                exit 1
        fi
else
        echo "clickhouse server start fail -> Please check xml files required in ${DATABASE_DIR} for each instance"
        exit 1

fi
}
function kill_instance_and_exit(){
        kill_instance
        exit 1
}
function clear_log(){
        if [ -d "$LOG_DIR" ]; then
                cd ${LOG_DIR} && rm -rf *
        fi
}

function gather_log_for_codec(){
        cd ${OUTPUT_DIR} && mkdir -p ${LOG_PACK_FILE}/${1}
        cp -rf ${LOG_DIR} ${OUTPUT_DIR}/${LOG_PACK_FILE}/${1}
}

function pack_log(){
        if [ -e "${OUTPUT_DIR}/run.log" ]; then
                cp ${OUTPUT_DIR}/run.log ${OUTPUT_DIR}/${LOG_PACK_FILE}/
        fi
        echo "Please check all log information in ${OUTPUT_DIR}/${LOG_PACK_FILE}"
}

function setup_check(){

        iax_dev_num=`accel-config list | grep iax | wc -l`
	if [ $iax_dev_num -eq 0 ] ;then
                iax_dev_num=`accel-config list | grep iax | wc -l`
                if [ $iax_dev_num -eq 0 ] ;then
                        echo "No IAA devices available -> Please check IAA hardware setup manually!"
                        #exit 1
                else
	                echo "IAA enabled devices number:$iax_dev_num"
                fi
	else
	        echo "IAA enabled devices number:$iax_dev_num"
	fi        
        libaccel_version=`accel-config -v`
        clickhouser_version=`clickhouse server --version`
        kernel_dxd_log=`dmesg | grep dxd`
        echo "libaccel_version:$libaccel_version"
        echo "clickhouser_version:$clickhouser_version"
        echo -e "idxd section in kernel log:\n$kernel_dxd_log"
}

setup_check
export CLICKHOUSE_WATCHDOG_ENABLE=0
for i in  ${CODEC_CONFIG[@]}
do
        clear_log
        codec=${i}
        echo "run test------------$codec"
        run_test $codec
        gather_log_for_codec $codec
done

pack_log
echo "Done."