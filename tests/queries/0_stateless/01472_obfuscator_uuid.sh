#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS t_uuid"
$CLICKHOUSE_CLIENT --query="CREATE TABLE t_uuid(Id UUID) ENGINE=MergeTree ORDER BY (Id)"
$CLICKHOUSE_CLIENT --query="INSERT INTO t_uuid VALUES ('3f5ffba3-19ff-4f3d-8861-60ae6e1fc1aa'),('4bd62524-e33c-43e5-882d-f1d96cf5561e'),('7a8b45d2-c18b-4e8c-89eb-abf5bee88931'),('45bb7333-965b-4526-870e-4f941edb025b'),('a4e72d0e-f9fa-465e-8d9d-151b9ced94df'),('cb5818ab-83b5-48a8-94b0-5177e30176d9'),('701e8006-fc9f-4496-80ba-efa6817b917b'),('e0936acf-6e8f-42aa-8f56-d1363476eece'),('239bb790-5293-40df-92ae-472294b6e178'),('508d0e80-729f-4e3b-9336-4c5c8792f6be'),('94abef70-f2d6-4f7b-ad60-3889409f1dac'),('b6f1ec08-8473-4fa2-b134-73db040b0d82'),('7e54dcae-0bb4-4c4f-a636-54a705fb8b40'),('d1d258c2-a35f-4c00-abfa-8addbcbc5471'),('7c74fbd8-bf79-46ee-adfe-96271040a4f7'),('41e3a274-eea9-41d8-a128-de5a6658fcfd'),('a72dc048-f72f-470e-b0f9-60cfad6e1157'),('40634f4f-37bf-44e4-ac7c-6f024ad19990')"
$CLICKHOUSE_CLIENT --query="SELECT Id FROM t_uuid ORDER BY (Id) FORMAT TSV" > "${CLICKHOUSE_TMP}"/data.tsv

echo FROM RAW DATA && cat "${CLICKHOUSE_TMP}"/data.tsv
echo TRANSFORMED TO && $CLICKHOUSE_OBFUSCATOR --structure "Id UUID" --input-format TSV --output-format TSV --seed dsrub < "${CLICKHOUSE_TMP}"/data.tsv 2>/dev/null

$CLICKHOUSE_CLIENT --query="DROP TABLE t_uuid"
rm "${CLICKHOUSE_TMP}"/data.tsv
