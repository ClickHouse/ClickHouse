#!/usr/bin/env bash
# Tags: no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/03273_shard_names_cluster.xml

## Test shard name literal
cat > "$config_path" <<EOL
<clickhouse>
    <remote_servers>
        <test_shard_names>
            <shard>
                <shard_name>first</shard_name>
                <replica>
                    <host>localhost</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <shard_name>second</shard_name>
                <replica>
                    <host>localhost</host>
                    <port>1</port>
                </replica>
            </shard>
        </test_shard_names>
    </remote_servers>
</clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query="SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names'"

## Test shard name
cat > "$config_path" <<EOL
<clickhouse>
    <remote_servers>
        <test_shard_names>
            <shard>
                <shard_name>3</shard_name>
                <replica>
                    <host>localhost</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <shard_name>10</shard_name>
                <replica>
                    <host>localhost</host>
                    <port>1</port>
                </replica>
            </shard>
        </test_shard_names>
    </remote_servers>
</clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query="SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names'"

## Test shard name for nodes
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
         <test_shard_names>
             <node>
                 <shard_name>3</shard_name>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
             <node>
                 <shard_name>6</shard_name>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
         </test_shard_names>
     </remote_servers>
 </clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG"
$CLICKHOUSE_CLIENT --query="SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names'"


# Test incorrect shard name, empty.
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
         <test_shard_names>
             <node>
                 <shard_name></shard_name>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
         </test_shard_names>
     </remote_servers>
 </clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG; -- { serverError INVALID_SHARD_ID }"

## Test incorrect static nodes numbers, dublicates.
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
         <test_shard_names>
             <node>
                 <shard_name>3</shard_name>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
             <node>
                 <shard_name>3</shard_name>
                 <host>localhost</host>
                 <port>9000</port>
             </node>
         </test_shard_names>
     </remote_servers>
 </clickhouse>
EOL
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG; -- { serverError INVALID_SHARD_ID }"

rm $config_path
$CLICKHOUSE_CLIENT --query="SYSTEM RELOAD CONFIG;"
