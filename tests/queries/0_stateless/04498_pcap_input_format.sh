#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PCAP format requires libtins/libpcap, which are not built in fasttest (ENABLE_LIBRARIES=0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Generate the fixtures into a private directory: the flaky check runs several
# copies of the test at the same time, and a shared directory would be rewritten
# while another copy is reading it.
DATA_DIR="$CLICKHOUSE_TMP/${CLICKHOUSE_TEST_UNIQUE_NAME}_pcap"
trap 'rm -rf "$DATA_DIR"' EXIT
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"
python3 "$CUR_DIR/data_pcap/generate.py" "$DATA_DIR"

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

echo "--- VLAN-tagged frame: eth_type unwraps the 802.1Q tag ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, eth_type, vlan_id, ip_protocol, src_port, dst_port
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE vlan_id IS NOT NULL
ORDER BY number FORMAT TSV"

echo "--- non-first IPv4 fragment: ip_protocol comes from the IPv4 header ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, ip_protocol, src_port, dst_port, tcp_flags
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE ip_protocol = 'TCP' AND src_port IS NULL
ORDER BY number FORMAT TSV"

echo "--- native IPv6: addresses, next header and ports ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, eth_type, ip_version, src_addr, dst_addr, ip_protocol, ip_ttl, src_port, dst_port, tcp_flags
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE ip_version = 6
ORDER BY number FORMAT TSV"

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

# A capture that does not come from a plain local file is copied into a
# temporary file first, because libpcap reads only from a file.
echo "--- from a stream: standard input ---"
$CLICKHOUSE_LOCAL --input-format PCAP -q "SELECT count(), countIf(ip_protocol = 'TCP') FROM table FORMAT TSV" < "$DATA_DIR/packets.pcap"

echo "--- from a stream: compressed file ---"
gzip -c "$DATA_DIR/packets.pcap" > "$DATA_DIR/packets.pcap.gz"
$CLICKHOUSE_LOCAL -q "
SELECT count(), countIf(ip_protocol = 'TCP') FROM file('$DATA_DIR/packets.pcap.gz', PCAP) FORMAT TSV"

echo "--- schema inference via file() extension is PCAP-explicit only (count) ---"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/packets.pcap', PCAP)"
