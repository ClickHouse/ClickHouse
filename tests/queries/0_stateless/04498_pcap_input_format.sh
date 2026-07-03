#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the PCAP format requires libtins/libpcap, which are not built in fasttest (ENABLE_LIBRARIES=0).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="$CUR_DIR/data_pcap"

echo "--- schema ---"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/http.pcap', PCAP) FORMAT TSV" | cut -f1,2

echo "--- classic pcap (HTTP over TCP) ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, link_type, protocols, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port, tcp_flags
FROM file('$DATA_DIR/http.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- pcapng (DHCP over UDP) ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, ip_protocol, src_port, dst_port
FROM file('$DATA_DIR/dhcp.pcapng', PCAP)
ORDER BY number FORMAT TSV"

echo "--- pcapng (ICMP) protocol/count ---"
$CLICKHOUSE_LOCAL -q "
SELECT ip_protocol, count()
FROM file('$DATA_DIR/icmp_ascii.pcapng', PCAP)
GROUP BY ip_protocol FORMAT TSV"

echo "--- tls pcap: all Ethernet/IPv4/TCP ---"
$CLICKHOUSE_LOCAL -q "
SELECT
    count() AS packets,
    countIf(link_type = 'ETHERNET_II') AS ethernet,
    countIf(ip_version = 4) AS ipv4,
    countIf(ip_protocol = 'TCP') AS tcp
FROM file('$DATA_DIR/tls13-20-chacha20poly1305.pcap', PCAP) FORMAT TSV"

echo "--- length invariants hold across all fixtures ---"
for f in http.pcap dhcp.pcapng icmp_ascii.pcapng tls13-20-chacha20poly1305.pcap; do
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
SELECT number, dst_port FROM file('$DATA_DIR/http.pcap', PCAP) ORDER BY number FORMAT TSV"

echo "--- schema inference via file() extension is PCAP-explicit only (count) ---"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/dhcp.pcapng', PCAP)"
