#!/bin/bash
ckhost="localhost"
ckport=("9000" "9001" "9002" "9003")
WORKING_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/.."
OUTPUT_DIR="${WORKING_DIR}/output"
LOG_DIR="${OUTPUT_DIR}/log"
RAWDATA_DIR="${WORKING_DIR}/rawdata_dir"
database_dir="${WORKING_DIR}/database_dir"
CLIENT_SCRIPTS_DIR="${WORKING_DIR}/client_scripts"
LOG_PACK_FILE="$(date +%Y-%m-%d-%H-%M-%S)"
QUERY_FILE="queries_ssb.sql"
SERVER_BIND_CMD[0]="numactl -m 0 -N 0"
SERVER_BIND_CMD[1]="numactl -m 0 -N 0"
SERVER_BIND_CMD[2]="numactl -m 1 -N 1"
SERVER_BIND_CMD[3]="numactl -m 1 -N 1"
CLIENT_BIND_CMD=""
SSB_GEN_FACTOR=20
TABLE_NAME="lineorder_flat"
TALBE_ROWS="119994608"
CODEC_CONFIG="lz4 deflate zstd"

# define instance number
inst_num=$1
if [ ! -n "$1" ]; then
        echo "Please clarify instance number from 1,2,3 or 4"
        exit 1
else
        echo "Benchmarking with instance number:$1"
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
    CREATE TABLE customer
    (
            C_CUSTKEY       UInt32,
            C_NAME          String,
            C_ADDRESS       String,
            C_CITY          LowCardinality(String),
            C_NATION        LowCardinality(String),
            C_REGION        LowCardinality(String),
            C_PHONE         String,
            C_MKTSEGMENT    LowCardinality(String)
    )
    ENGINE = MergeTree ORDER BY (C_CUSTKEY);

    CREATE TABLE lineorder
    (
        LO_ORDERKEY             UInt32,
        LO_LINENUMBER           UInt8,
        LO_CUSTKEY              UInt32,
        LO_PARTKEY              UInt32,
        LO_SUPPKEY              UInt32,
        LO_ORDERDATE            Date,
        LO_ORDERPRIORITY        LowCardinality(String),
        LO_SHIPPRIORITY         UInt8,
        LO_QUANTITY             UInt8,
        LO_EXTENDEDPRICE        UInt32,
        LO_ORDTOTALPRICE        UInt32,
        LO_DISCOUNT             UInt8,
        LO_REVENUE              UInt32,
        LO_SUPPLYCOST           UInt32,
        LO_TAX                  UInt8,
        LO_COMMITDATE           Date,
        LO_SHIPMODE             LowCardinality(String)
    )
    ENGINE = MergeTree PARTITION BY toYear(LO_ORDERDATE) ORDER BY (LO_ORDERDATE, LO_ORDERKEY);

    CREATE TABLE part
    (
            P_PARTKEY       UInt32,
            P_NAME          String,
            P_MFGR          LowCardinality(String),
            P_CATEGORY      LowCardinality(String),
            P_BRAND         LowCardinality(String),
            P_COLOR         LowCardinality(String),
            P_TYPE          LowCardinality(String),
            P_SIZE          UInt8,
            P_CONTAINER     LowCardinality(String)
    )
    ENGINE = MergeTree ORDER BY P_PARTKEY;

    CREATE TABLE supplier
    (
            S_SUPPKEY       UInt32,
            S_NAME          String,
            S_ADDRESS       String,
            S_CITY          LowCardinality(String),
            S_NATION        LowCardinality(String),
            S_REGION        LowCardinality(String),
            S_PHONE         String
    )
    ENGINE = MergeTree ORDER BY S_SUPPKEY;
"
supplier_table="
   CREATE TABLE supplier
    (
            S_SUPPKEY       UInt32,
            S_NAME          String,
            S_ADDRESS       String,
            S_CITY          LowCardinality(String),
            S_NATION        LowCardinality(String),
            S_REGION        LowCardinality(String),
            S_PHONE         String
    )
    ENGINE = MergeTree ORDER BY S_SUPPKEY;
"
part_table="
    CREATE TABLE part
    (
            P_PARTKEY       UInt32,
            P_NAME          String,
            P_MFGR          LowCardinality(String),
            P_CATEGORY      LowCardinality(String),
            P_BRAND         LowCardinality(String),
            P_COLOR         LowCardinality(String),
            P_TYPE          LowCardinality(String),
            P_SIZE          UInt8,
            P_CONTAINER     LowCardinality(String)
    )
    ENGINE = MergeTree ORDER BY P_PARTKEY;
"
lineorder_table="
    CREATE TABLE lineorder
    (
        LO_ORDERKEY             UInt32,
        LO_LINENUMBER           UInt8,
        LO_CUSTKEY              UInt32,
        LO_PARTKEY              UInt32,
        LO_SUPPKEY              UInt32,
        LO_ORDERDATE            Date,
        LO_ORDERPRIORITY        LowCardinality(String),
        LO_SHIPPRIORITY         UInt8,
        LO_QUANTITY             UInt8,
        LO_EXTENDEDPRICE        UInt32,
        LO_ORDTOTALPRICE        UInt32,
        LO_DISCOUNT             UInt8,
        LO_REVENUE              UInt32,
        LO_SUPPLYCOST           UInt32,
        LO_TAX                  UInt8,
        LO_COMMITDATE           Date,
        LO_SHIPMODE             LowCardinality(String)
    )
    ENGINE = MergeTree PARTITION BY toYear(LO_ORDERDATE) ORDER BY (LO_ORDERDATE, LO_ORDERKEY);
"
customer_table="
    CREATE TABLE customer
    (
            C_CUSTKEY       UInt32,
            C_NAME          String,
            C_ADDRESS       String,
            C_CITY          LowCardinality(String),
            C_NATION        LowCardinality(String),
            C_REGION        LowCardinality(String),
            C_PHONE         String,
            C_MKTSEGMENT    LowCardinality(String)
    )
    ENGINE = MergeTree ORDER BY (C_CUSTKEY);
"

lineorder_flat_table="
    SET max_memory_usage = 20000000000;
    CREATE TABLE lineorder_flat
    ENGINE = MergeTree
    PARTITION BY toYear(LO_ORDERDATE)
    ORDER BY (LO_ORDERDATE, LO_ORDERKEY) AS
    SELECT
        l.LO_ORDERKEY AS LO_ORDERKEY,
        l.LO_LINENUMBER AS LO_LINENUMBER,
        l.LO_CUSTKEY AS LO_CUSTKEY,
        l.LO_PARTKEY AS LO_PARTKEY,
        l.LO_SUPPKEY AS LO_SUPPKEY,
        l.LO_ORDERDATE AS LO_ORDERDATE,
        l.LO_ORDERPRIORITY AS LO_ORDERPRIORITY,
        l.LO_SHIPPRIORITY AS LO_SHIPPRIORITY,
        l.LO_QUANTITY AS LO_QUANTITY,
        l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
        l.LO_ORDTOTALPRICE AS LO_ORDTOTALPRICE,
        l.LO_DISCOUNT AS LO_DISCOUNT,
        l.LO_REVENUE AS LO_REVENUE,
        l.LO_SUPPLYCOST AS LO_SUPPLYCOST,
        l.LO_TAX AS LO_TAX,
        l.LO_COMMITDATE AS LO_COMMITDATE,
        l.LO_SHIPMODE AS LO_SHIPMODE,
        c.C_NAME AS C_NAME,
        c.C_ADDRESS AS C_ADDRESS,
        c.C_CITY AS C_CITY,
        c.C_NATION AS C_NATION,
        c.C_REGION AS C_REGION,
        c.C_PHONE AS C_PHONE,
        c.C_MKTSEGMENT AS C_MKTSEGMENT,
        s.S_NAME AS S_NAME,
        s.S_ADDRESS AS S_ADDRESS,
        s.S_CITY AS S_CITY,
        s.S_NATION AS S_NATION,
        s.S_REGION AS S_REGION,
        s.S_PHONE AS S_PHONE,
        p.P_NAME AS P_NAME,
        p.P_MFGR AS P_MFGR,
        p.P_CATEGORY AS P_CATEGORY,
        p.P_BRAND AS P_BRAND,
        p.P_COLOR AS P_COLOR,
        p.P_TYPE AS P_TYPE,
        p.P_SIZE AS P_SIZE,
        p.P_CONTAINER AS P_CONTAINER
    FROM lineorder AS l
    INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
    INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
    INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY;
    show settings ilike 'max_memory_usage';
"
 
function insert_data(){
        echo "insert_data:$1"
        create_table_prefix="clickhouse client --host ${ckhost} --port $2 --multiquery -q"
        insert_data_prefix="clickhouse client --query "
        case $1 in
          all)
                clickhouse client --host ${ckhost} --port $2 --multiquery -q"$ckreadSql" && {
                ${insert_data_prefix} "INSERT INTO customer FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/customer.tbl --port=$2
                ${insert_data_prefix} "INSERT INTO part FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/part.tbl --port=$2
                ${insert_data_prefix} "INSERT INTO supplier FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/supplier.tbl --port=$2
                ${insert_data_prefix} "INSERT INTO lineorder FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/lineorder.tbl --port=$2
                }
                ${create_table_prefix}"${lineorder_flat_table}" 
          ;;
          customer)
                echo ${create_table_prefix}\"${customer_table}\"
                ${create_table_prefix}"${customer_table}" && {
                echo "${insert_data_prefix} \"INSERT INTO $1 FORMAT CSV\" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2"
                ${insert_data_prefix} "INSERT INTO $1 FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2
                }
          ;;
          part)
                echo ${create_table_prefix}\"${part_table}\"
                ${create_table_prefix}"${part_table}" && {
                echo "${insert_data_prefix} \"INSERT INTO $1 FORMAT CSV\" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2"
                ${insert_data_prefix} "INSERT INTO $1 FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2
                }
          ;;
          supplier)
                echo ${create_table_prefix}"${supplier_table}"
                ${create_table_prefix}"${supplier_table}" && {
                echo "${insert_data_prefix} \"INSERT INTO $1 FORMAT CSV\" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2"
                ${insert_data_prefix} "INSERT INTO $1 FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2
                }
          ;;
          lineorder)
                echo ${create_table_prefix}"${lineorder_table}"
                ${create_table_prefix}"${lineorder_table}" && {
                echo "${insert_data_prefix} \"INSERT INTO $1 FORMAT CSV\" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2"
                ${insert_data_prefix} "INSERT INTO $1 FORMAT CSV" < ${RAWDATA_DIR}/ssb-dbgen/$1.tbl --port=$2
                }
          ;;
          lineorder_flat)
                echo ${create_table_prefix}"${lineorder_flat_table}"
                ${create_table_prefix}"${lineorder_flat_table}" 
                return 0
          ;;
          *)
                exit 0
                ;;

        esac
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
                        exit 1
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
                echo "cd ${database_dir}/$1${dir_server[i]}"
                echo "${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&${LOG_DIR}/${1}_${i}_server_log& > /dev/null"
                
	        cd ${database_dir}/$1${dir_server[i]}
	        ${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&${LOG_DIR}/${1}_${i}_server_log& > /dev/null
                check_instance ${ckport[i]}
        done
}

function start_clickhouse_for_stressing(){
        echo "start_clickhouse_for_stressing"
        for i in $(seq 0 $[inst_num-1])
	do
                echo "cd ${database_dir}/$1${dir_server[i]}"
                echo "${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&/dev/null&"
                
	        cd ${database_dir}/$1${dir_server[i]}
	        ${SERVER_BIND_CMD[i]} clickhouse server -C config_${1}${dir_server[i]}.xml >&/dev/null&
                check_instance ${ckport[i]}
        done
}
yum -y install git make gcc sudo net-tools &> /dev/null
pip3 install clickhouse_driver numpy &> /dev/null
test -d ${RAWDATA_DIR}/ssb-dbgen || git clone https://github.com/vadimtk/ssb-dbgen.git ${RAWDATA_DIR}/ssb-dbgen && cd ${RAWDATA_DIR}/ssb-dbgen

if [ ! -f ${RAWDATA_DIR}/ssb-dbgen/dbgen ];then
        make && {
        test -f ${RAWDATA_DIR}/ssb-dbgen/customer.tbl || echo y |./dbgen -s ${SSB_GEN_FACTOR} -T c
        test -f ${RAWDATA_DIR}/ssb-dbgen/part.tbl  || echo y | ./dbgen -s ${SSB_GEN_FACTOR} -T p
        test -f ${RAWDATA_DIR}/ssb-dbgen/supplier.tbl || echo y | ./dbgen -s ${SSB_GEN_FACTOR} -T s
        test -f ${RAWDATA_DIR}/ssb-dbgen/date.tbl || echo y | ./dbgen -s ${SSB_GEN_FACTOR} -T d
        test -f ${RAWDATA_DIR}/ssb-dbgen/lineorder.tbl || echo y | ./dbgen -s ${SSB_GEN_FACTOR} -T l
        }
else
        test -f ${RAWDATA_DIR}/ssb-dbgen/customer.tbl || echo y | ./dbgen -s ${SSB_GEN_FACTOR} -T c
        test -f ${RAWDATA_DIR}/ssb-dbgen/part.tbl  || echo y |  ./dbgen -s ${SSB_GEN_FACTOR} -T p
        test -f ${RAWDATA_DIR}/ssb-dbgen/supplier.tbl || echo y | ./dbgen -s ${SSB_GEN_FACTOR} -T s
        test -f ${RAWDATA_DIR}/ssb-dbgen/date.tbl || echo y |  ./dbgen -s ${SSB_GEN_FACTOR} -T d
        test -f ${RAWDATA_DIR}/ssb-dbgen/lineorder.tbl || echo y | ./dbgen -s ${SSB_GEN_FACTOR} -T l

fi

filenum=`find ${RAWDATA_DIR}/ssb-dbgen/ -name "*.tbl" | wc -l`

if [ $filenum -ne 5 ];then
        echo "generate ssb data file *.tbl faild"
        exit 1
fi

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
        if [ -f ${database_dir}/${1}${dir_server[i]}/config_${1}${dir_server[i]}.xml ]; then
                is_xml=$[is_xml+1]
        fi
done
if [ $is_xml -eq $inst_num ];then
        echo "Benchmark with $inst_num instance"
        start_clickhouse_for_insertion ${1}

        for i in $(seq 0 $[inst_num-1])
        do
                clickhouse client --host ${ckhost} --port ${ckport[i]} -m -q"show databases;" >/dev/null
        done

        if [ $? -eq 0 ];then
                check_table
        fi
        kill_instance

        if [ $1 == "deflate" ];then
	        test -f ${LOG_DIR}/${1}_server_log && deflatemsg=`cat ${LOG_DIR}/${1}_server_log | grep DeflateJobHWPool`
	        if [ -n "$deflatemsg" ];then
	                echo ------------------------------------------------------
	                echo $deflatemsg
	                echo ------------------------------------------------------
	        fi
	fi
        echo "Check table data required in server_${1} -> Done! "
        
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
                        kill_instance
	                test -f ${LOG_DIR}/${1}.log && echo  "${1}.log ===> ${LOG_DIR}/${1}.log"
	        else
	                kill_instance
	                echo "No find 'Finished' in client log -> Performance test may fail"
	                exit 1

	        fi

	    else
                echo "${1} clickhouse server start fail"
                exit 1
        fi
else
        echo "clickhouse server start fail -> Please check xml files required in ${database_dir} for each instance"
        exit 1

fi
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
                        exit 1
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