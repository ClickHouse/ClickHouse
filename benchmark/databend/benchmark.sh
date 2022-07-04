#!/bin/bash

mkdir databend && cd databend
curl -LJO 'https://github.com/datafuselabs/databend/releases/download/v0.7.113-nightly/databend-v0.7.113-nightly-x86_64-unknown-linux-musl.tar.gz'
tar xzvf 'databend-v0.7.113-nightly-x86_64-unknown-linux-musl.tar.gz'

echo 'dir = "metadata/_logs"
admin_api_address = "127.0.0.1:8101"
grpc_api_address = "127.0.0.1:9101"

[raft_config]
id = 1
single = true
raft_dir = "metadata/datas"' > databend-meta.toml

./bin/databend-meta -c ./databend-meta.toml > meta.log 2>&1 &
curl -I 'http://127.0.0.1:8101/v1/health'

echo '[log]
level = "INFO"
dir = "benddata/_logs"

[query]
# For admin RESET API.
admin_api_address = "127.0.0.1:8001"

# Metrics.
metric_api_address = "127.0.0.1:7071"

# Cluster flight RPC.
flight_api_address = "127.0.0.1:9091"

# Query MySQL Handler.
mysql_handler_host = "127.0.0.1"
mysql_handler_port = 3307

# Query ClickHouse Handler.
clickhouse_handler_host = "127.0.0.1"
clickhouse_handler_port = 9001

# Query ClickHouse HTTP Handler.
clickhouse_http_handler_host = "127.0.0.1"
clickhouse_http_handler_port = 8125

# Query HTTP Handler.
http_handler_host = "127.0.0.1"
http_handler_port = 8081

tenant_id = "tenant1"
cluster_id = "cluster1"

[meta]
# databend-meta grpc api address.
address = "127.0.0.1:9101"
username = "root"
password = "root"

[storage]
# fs|s3
type = "fs"

[storage.fs]
data_path = "benddata/datas"' > databend-query.toml

./bin/databend-query -c ./databend-query.toml > query.log 2>&1 &

curl https://clickhouse.com/ | sh
sudo ./clickhouse install

# Load the data

curl 'http://default@localhost:8124/' --data-binary @create.sql

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

# Note: if I run
# clickhouse-client --time --query "INSERT INTO hits FORMAT TSV" < hits.tsv
# it panics:
# ERROR common_tracing::panic_hook: panicked at 'called `Result::unwrap()` on an `Err` value: SendError

# Note: if I run
# curl -XPUT 'http://root:@127.0.0.1:8000/v1/streaming_load' -H 'insert_sql: insert into hits format CSV' -H 'skip_header: 0' -H 'field_delimiter: ,' -H 'record_delimiter: \n' -F 'upload=@"./hits.csv"'
# curl: (55) Send failure: Broken pipe

# This is not entirely correct, but starts to work:
# curl -XPUT 'http://root:@127.0.0.1:8000/v1/streaming_load' -H 'insert_sql: insert into hits format TSV' -H 'skip_header: 0' -H 'field_delimiter: \t' -H 'record_delimiter: \n' -F 'upload=@"./hits.tsv"'
# and fails after 7 minutes 38 seconds without loading any data:
# Code: 4000, displayText = invalid data (Expected to have terminated string literal.) (while in processor thread 5).
# the diagnostics is terrible.

head -n 90000000 hits.tsv > hits90m.tsv
time curl -XPUT 'http://root:@127.0.0.1:8000/v1/streaming_load' -H 'insert_sql: insert into hits format TSV' -H 'skip_header: 0' -H 'field_delimiter: \t' -H 'record_delimiter: \n' -F 'upload=@"./hits90m.tsv"'

# {"id":"08f59e6c-2924-483e-bb96-cbcb458588f5","state":"SUCCESS","stats":{"rows":90000000,"bytes":73152552024},"error":null}
# real    7m15.312s

du -bcs _data
# 38714978944

# It does not support ClickHouse protocol well (it hangs on some queries if they are too long).

./run.sh 2>&1 | tee log.txt

# Note: divide every number by 0.9 as only 90% of the data was loaded successfully.
