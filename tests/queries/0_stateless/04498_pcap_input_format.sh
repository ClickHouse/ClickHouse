#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PCAP format requires libtins/libpcap, which are not built in fasttest (ENABLE_LIBRARIES=0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="$CUR_DIR/data_pcap"
python3 "$DATA_DIR/generate.py"

echo "--- schema ---"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/packets.pcap', PCAP) FORMAT TSV" | cut -f1,2

echo "--- classic pcap (HTTP over TCP) ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, link_type, protocols, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port, tcp_flags
FROM file('$DATA_DIR/packets.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- UDP ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, ip_protocol, src_port, dst_port
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE ip_protocol = 'UDP' FORMAT TSV"

echo "--- ICMP protocol/count ---"
$CLICKHOUSE_LOCAL -q "
SELECT ip_protocol, count()
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE ip_protocol = 'ICMP'
GROUP BY ip_protocol FORMAT TSV"

echo "--- all Ethernet/IPv4/TCP ---"
$CLICKHOUSE_LOCAL -q "
SELECT
    count() AS packets,
    countIf(link_type = 'ETHERNET_II') AS ethernet,
    countIf(ip_version = 4) AS ipv4,
    countIf(ip_protocol = 'TCP') AS tcp
FROM file('$DATA_DIR/packets.pcap', PCAP) FORMAT TSV"

echo "--- pcapng reads identically to the classic pcap ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, link_type, protocols, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port, tcp_flags
FROM file('$DATA_DIR/packets.pcapng', PCAP)
ORDER BY number FORMAT TSV"

echo "--- pcapng and pcap agree on every column ---"
$CLICKHOUSE_LOCAL -q "
SELECT
    (SELECT groupArray(tuple(*)) FROM (SELECT * FROM file('$DATA_DIR/packets.pcap', PCAP) ORDER BY number))
    = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM file('$DATA_DIR/packets.pcapng', PCAP) ORDER BY number)) FORMAT TSV"

echo "--- truncated capture (snaplen 34: original_length > capture_length) ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, capture_length, original_length, protocols, ip_protocol, length(raw)
FROM file('$DATA_DIR/truncated.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- length invariants hold across all fixtures ---"
for f in packets.pcap packets.pcapng truncated.pcap; do
    $CLICKHOUSE_LOCAL -q "
    SELECT
        '$f',
        countIf(length(raw) != capture_length) AS raw_mismatch,
        countIf(original_length < capture_length) AS orig_lt_cap,
        countIf(payload_length > 0 AND substring(raw, (capture_length - payload_length) + 1) != payload) AS payload_mismatch
    FROM file('$DATA_DIR/$f', PCAP) FORMAT TSV"
done

echo "--- subset of columns (only number, dst_port) ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, dst_port FROM file('$DATA_DIR/packets.pcap', PCAP) ORDER BY number FORMAT TSV"

echo "--- schema inference via file() extension is PCAP-explicit only (count) ---"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/packets.pcap', PCAP)"
