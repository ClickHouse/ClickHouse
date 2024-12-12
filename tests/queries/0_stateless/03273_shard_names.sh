#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

config_name=03273_shard_names_cluster.xml
config_reload_name=config_reload_interval.xml

config_path=${CLICKHOUSE_CONFIG_DIR}/config.d/${config_name}
config_reload_path=${CLICKHOUSE_CONFIG_DIR}/config.d/${config_reload_name}

rm -f $config_path
rm -f $config_reload_path

cat > "$config_reload_path" <<EOL
<clickhouse>
    <config_reload_interval_ms>200</config_reload_interval_ms>
</clickhouse>
EOL

## config_reload_interval_ms * 3.
sleep_conf_reload=0.6
## Waiting while config_reload_interval_ms updates. Default 2 sec, 2 * 2 sec should be enough.
sleep 4

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
sleep $sleep_conf_reload
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
sleep $sleep_conf_reload
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
sleep $sleep_conf_reload
$CLICKHOUSE_CLIENT --query="SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names'"


# Test incorrect shard name, empty.
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
        <test_shard_names_incorrect_empty_name>
            <node>
                <shard_name></shard_name>
                <host>localhost</host>
                <port>9000</port>
            </node>
        </test_shard_names_incorrect_empty_name>
     </remote_servers>
 </clickhouse>
EOL
sleep $sleep_conf_reload
## Result must be empty.
$CLICKHOUSE_CLIENT --query="SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names_incorrect_empty_name'"

## Test incorrect static nodes numbers, dublicates.
cat > "$config_path" <<EOL
<clickhouse>
     <remote_servers>
         <test_shard_names_incorrect_non_unique>
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
         </test_shard_names_incorrect_non_unique>
     </remote_servers>
 </clickhouse>
EOL
sleep $sleep_conf_reload
## Result must be empty.
$CLICKHOUSE_CLIENT --query="SELECT shard_name FROM system.clusters WHERE cluster='test_shard_names_incorrect_non_unique'"

rm $config_reload_path
rm $config_path
sleep $sleep_conf_reload
