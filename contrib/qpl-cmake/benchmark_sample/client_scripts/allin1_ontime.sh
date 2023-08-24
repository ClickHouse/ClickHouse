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
ckreadSql="
CREATE TABLE default.ontime
(
    Year                            UInt16,
    Quarter                         UInt8,
    Month                           UInt8,
    DayofMonth                      UInt8,
    DayOfWeek                       UInt8,
    FlightDate                      Date,
    Reporting_Airline               LowCardinality(String),
    DOT_ID_Reporting_Airline        Int32,
    IATA_CODE_Reporting_Airline     LowCardinality(String),
    Tail_Number                     LowCardinality(String),
    Flight_Number_Reporting_Airline LowCardinality(String),
    OriginAirportID                 Int32,
    OriginAirportSeqID              Int32,
    OriginCityMarketID              Int32,
    Origin                          FixedString(5),
    OriginCityName                  LowCardinality(String),
    OriginState                     FixedString(2),
    OriginStateFips                 FixedString(2),
    OriginStateName                 LowCardinality(String),
    OriginWac                       Int32,
    DestAirportID                   Int32,
    DestAirportSeqID                Int32,
    DestCityMarketID                Int32,
    Dest                            FixedString(5),
    DestCityName                    LowCardinality(String),
    DestState                       FixedString(2),
    DestStateFips                   FixedString(2),
    DestStateName                   LowCardinality(String),
    DestWac                         Int32,
    CRSDepTime                      Int32,
    DepTime                         Int32,
    DepDelay                        Int32,
    DepDelayMinutes                 Int32,
    DepDel15                        Int32,
    DepartureDelayGroups            LowCardinality(String),
    DepTimeBlk                      LowCardinality(String),
    TaxiOut                         Int32,
    WheelsOff                       LowCardinality(String),
    WheelsOn                        LowCardinality(String),
    TaxiIn                          Int32,
    CRSArrTime                      Int32,
    ArrTime                         Int32,
    ArrDelay                        Int32,
    ArrDelayMinutes                 Int32,
    ArrDel15                        Int32,
    ArrivalDelayGroups              LowCardinality(String),
    ArrTimeBlk                      LowCardinality(String),
    Cancelled                       Int8,
    CancellationCode                FixedString(1),
    Diverted                        Int8,
    CRSElapsedTime                  Int32,
    ActualElapsedTime               Int32,
    AirTime                         Int32,
    Flights                         Int32,
    Distance                        Int32,
    DistanceGroup                   Int8,
    CarrierDelay                    Int32,
    WeatherDelay                    Int32,
    NASDelay                        Int32,
    SecurityDelay                   Int32,
    LateAircraftDelay               Int32,
    FirstDepTime                    Int16,
    TotalAddGTime                   Int16,
    LongestAddGTime                 Int16,
    DivAirportLandings              Int8,
    DivReachedDest                  Int8,
    DivActualElapsedTime            Int16,
    DivArrDelay                     Int16,
    DivDistance                     Int16,
    Div1Airport                     LowCardinality(String),
    Div1AirportID                   Int32,
    Div1AirportSeqID                Int32,
    Div1WheelsOn                    Int16,
    Div1TotalGTime                  Int16,
    Div1LongestGTime                Int16,
    Div1WheelsOff                   Int16,
    Div1TailNum                     LowCardinality(String),
    Div2Airport                     LowCardinality(String),
    Div2AirportID                   Int32,
    Div2AirportSeqID                Int32,
    Div2WheelsOn                    Int16,
    Div2TotalGTime                  Int16,
    Div2LongestGTime                Int16,
    Div2WheelsOff                   Int16,
    Div2TailNum                     LowCardinality(String),
    Div3Airport                     LowCardinality(String),
    Div3AirportID                   Int32,
    Div3AirportSeqID                Int32,
    Div3WheelsOn                    Int16,
    Div3TotalGTime                  Int16,
    Div3LongestGTime                Int16,
    Div3WheelsOff                   Int16,
    Div3TailNum                     LowCardinality(String),
    Div4Airport                     LowCardinality(String),
    Div4AirportID                   Int32,
    Div4AirportSeqID                Int32,
    Div4WheelsOn                    Int16,
    Div4TotalGTime                  Int16,
    Div4LongestGTime                Int16,
    Div4WheelsOff                   Int16,
    Div4TailNum                     LowCardinality(String),
    Div5Airport                     LowCardinality(String),
    Div5AirportID                   Int32,
    Div5AirportSeqID                Int32,
    Div5WheelsOn                    Int16,
    Div5TotalGTime                  Int16,
    Div5LongestGTime                Int16,
    Div5WheelsOff                   Int16,
    Div5TailNum                     LowCardinality(String)
) ENGINE = MergeTree
  ORDER BY (Year, Quarter, Month, DayofMonth, FlightDate, IATA_CODE_Reporting_Airline);
"

function check_table(){
        checknum=0
        sql_check_table_exist="SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'default' AND table_name = '${TABLE_NAME}');"
        echo "Checking table data required in server..."
        for i in $(seq 0 $[inst_num-1])
        do
                var=$(clickhouse client --host ${ckhost} --port ${ckport[i]} -m -q"${sql_check_table_exist}")
                if [ $var -eq 0 ];then
                        echo "table not exist,create table and insert data..."
                        let checknum+=1 && {
                                clickhouse client --host ${ckhost} --port ${ckport[i]} --multiquery -q"$ckreadSql"
                                clickhouse client --query "insert into default.ontime FORMAT CSV" < ${RAWDATA_DIR}/ontime.csv --port=${ckport[i]}
                        }
                else
                        var=$(clickhouse client --host ${ckhost} --port ${ckport[i]} -m -q"select count() from ${TABLE_NAME};")
                        if [ $var -eq 0 ];then
                                echo "table exist,but 0 row! insert data..."
                                let checknum+=1 && {
                                        clickhouse client --query "insert into default.ontime FORMAT CSV" < ${RAWDATA_DIR}/ontime.csv --port=${ckport[i]}
                                }
                        else
                                if [ $var -eq $TALBE_ROWS ];then
                                        echo "Instance_${i} Table data integrity check OK -> Rows:$var"
                                else
                                        echo  "Instance_${i} Table data integrity check Failed -> Rows:$var"
                                        kill_instance_and_exit
                                fi
                        fi
                fi
        done

        if [ $checknum -gt 0 ];then
                echo "check_table Done! Need sleep 10s after first table data insertion...$checknum"
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

function check_and_generate_rawdata(){
echo "check if need generate rawdata: ontime.csv..."

folder_size_mb=$(echo "scale=0; $(du -sk "$DATABASE_DIR" | awk '{print $1}') / 1024" | bc)
if [ $folder_size_mb -lt 1 ]; then
        echo "Size of $DATABASE_DIR: $folder_size_mb MB too small, conclude this is first time to run benchmark, hence need generate rawdata..."
        if [ ! -f "${RAWDATA_DIR}/ontime.csv" ]; then
                test -d ${RAWDATA_DIR} && cd ${RAWDATA_DIR} && curl -O https://clickhouse-datasets.s3.yandex.net/ontime/partitions/ontime.tar
                test -f ${RAWDATA_DIR}/ontime.tar && tar -xvf ontime.tar
                kill_instance
                cd ${RAWDATA_DIR} && clickhouse server >&/dev/null&
                echo "Need sleep 10s for partitions data warming..."
                sleep 10
                echo "Export partitions data into CSV..."
                clickhouse client --query="select * from datasets.ontime FORMAT CSV" > ${RAWDATA_DIR}/ontime.csv
                test ! -f ${RAWDATA_DIR}/ontime.csv && echo "generate ontime.csv faild!" && exit 1
                echo "ontime.csv generate ok!"
                kill_instance
        else
                echo "ontime.csv already exist!"
        fi
fi
}

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
check_and_generate_rawdata
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