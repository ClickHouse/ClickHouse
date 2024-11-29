#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/static_shards_cluster.xml

## Test static shard numbers
cat > "$config_path" <<EOL
<clickhouse>
    <remote_servers>
        <test_static_shard_nums>
            <shard>
                <shard_number>3</shard_number>
                <replica>
                    <host>localhost</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <shard_number>10</shard_number>
                <replica>
                    <host>localhost</host>
                    <port>1</port>
                </replica>
            </shard>
        </test_static_shard_nums>
    </remote_servers>
</clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query="SELECT shard_num FROM system.clusters WHERE cluster='test_static_shard_nums'"

## Test static nodes numbers
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
         <test_static_nodes_nums>
             <node>
                 <shard_number>3</shard_number>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
             <node>
                 <shard_number>6</shard_number>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
         </test_static_nodes_nums>
     </remote_servers>
 </clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query="SELECT shard_num FROM system.clusters WHERE cluster='test_static_nodes_nums'"


## Test incorrect shard number, 0-based.
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
         <test_static_nodes_nums>
             <node>
                 <shard_number>0</shard_number>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
         </test_static_nodes_nums>
     </remote_servers>
 </clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG; -- { serverError 577 }" 

## Test incorrect static nodes numbers, dublicates.
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
         <test_static_nodes_nums>
             <node>
                 <shard_number>3</shard_number>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
             <node>
                 <shard_number>3</shard_number>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
         </test_static_nodes_nums>
     </remote_servers>
 </clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG; -- { serverError 577 }" 

rm $config_path
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG;"
