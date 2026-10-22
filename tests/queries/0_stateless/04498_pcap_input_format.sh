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

echo "--- IPv6 extension headers: ip_protocol is the protocol after the chain ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, ip_protocol, src_port, dst_port, tcp_flags
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE ip_version = 6 AND number > 7
ORDER BY number FORMAT TSV"

echo "--- protocol filters see extension-header traffic ---"
$CLICKHOUSE_LOCAL -q "
SELECT ip_protocol, count(), countIf(src_port IS NULL)
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE ip_protocol IN ('TCP', 'UDP')
GROUP BY ip_protocol
ORDER BY ip_protocol FORMAT TSV"

echo "--- block size limits: rows, query rows, bytes, and both default ---"
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count() FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS input_format_pcap_max_block_size = 4 FORMAT TSV"
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count() FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS max_block_size = 4 FORMAT TSV"
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count(), sum(length(raw)) FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS input_format_pcap_prefer_block_bytes = 1 FORMAT TSV"
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count(), sum(length(raw)) FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS input_format_pcap_prefer_block_bytes = 100 FORMAT TSV"
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count(), sum(length(raw)) FROM file('$DATA_DIR/packets.pcap', PCAP) FORMAT TSV"
# The copied `payload` bytes count towards the byte budget too, so reading `payload` makes more blocks.
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count(), sum(length(raw)) FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS input_format_pcap_prefer_block_bytes = 80 FORMAT TSV"
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count(), sum(length(raw)), sum(length(payload)) FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS input_format_pcap_prefer_block_bytes = 80 FORMAT TSV"
# Only the stored packet bytes count: without `raw` only the `payload` bytes are charged, and metadata-only
# reads are not split by bytes at all.
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count(), sum(length(payload)) FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS input_format_pcap_prefer_block_bytes = 80 FORMAT TSV"
$CLICKHOUSE_LOCAL -q "
SELECT uniqExact(blockNumber()), count(), sum(capture_length) FROM file('$DATA_DIR/packets.pcap', PCAP)
SETTINGS input_format_pcap_prefer_block_bytes = 1 FORMAT TSV"

echo "--- truncated capture (snaplen 34: original_length > capture_length) ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, capture_length, original_length, protocols, ip_protocol, length(raw)
FROM file('$DATA_DIR/truncated.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- headers cut by the snapshot length: dissected up to the last complete header ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, capture_length, original_length, protocols, ip_version, ip_protocol, src_port
FROM file('$DATA_DIR/truncated_headers.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- headers cut by the snapshot length before the end of the IP header: the rest is raw, the outer protocols are kept ---"
for f in truncated_ip.pcap truncated_raw_ip.pcap truncated_dot3.pcap; do
    $CLICKHOUSE_LOCAL -q "
    SELECT number, capture_length, original_length, link_type, protocols, eth_src, eth_type, vlan_id, ip_version, length(raw)
    FROM file('$DATA_DIR/$f', PCAP)
    ORDER BY number FORMAT TSV"
done

echo "--- length invariants hold across all fixtures ---"
for f in packets.pcap packets.pcapng truncated.pcap truncated_headers.pcap truncated_ip.pcap truncated_raw_ip.pcap raw_ip.pcap ipv4.pcap ipv6.pcap null.pcap sll.pcap truncated_null.pcap truncated_sll.pcap ppi.pcap sll2.pcap dot3.pcap truncated_dot3.pcap options.pcap; do
    $CLICKHOUSE_LOCAL -q "
    SELECT
        '$f',
        countIf(length(raw) != capture_length) AS raw_mismatch,
        countIf(original_length < capture_length) AS orig_lt_cap,
        countIf(payload_length > 0 AND substring(raw, (capture_length - payload_length) + 1) != payload) AS payload_mismatch
    FROM file('$DATA_DIR/$f', PCAP) FORMAT TSV"
done

echo "--- padded short frame: payload excludes the Ethernet padding ---"
$CLICKHOUSE_LOCAL -q "
SELECT
    capture_length,
    payload_length,
    payload,
    substring(raw, (capture_length - payload_length) + 1) = payload AS payload_is_suffix,
    position(raw, payload) - 1 AS payload_offset
FROM file('$DATA_DIR/padded.pcap', PCAP) FORMAT TSV"

echo "--- tunnels: IP columns describe the outer header ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, eth_type, ip_version, src_addr, dst_addr, ip_protocol, ip_ttl, src_port, dst_port
FROM file('$DATA_DIR/tunnel.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- tunneled Ethernet: Ethernet columns describe the outer link layer ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, link_type, eth_src, eth_dst, eth_type, vlan_id, ip_protocol, dst_port
FROM file('$DATA_DIR/vxlan.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- subset of columns (only number, dst_port) ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, dst_port FROM file('$DATA_DIR/packets.pcap', PCAP) ORDER BY number FORMAT TSV"

# A capture that does not come from a plain local file is read by libpcap
# through a stream over the input buffer, without being spooled anywhere.
echo "--- from a stream: standard input ---"
$CLICKHOUSE_LOCAL --input-format PCAP -q "SELECT count(), countIf(ip_protocol = 'TCP') FROM table FORMAT TSV" < "$DATA_DIR/packets.pcap"

echo "--- from a stream: compressed file ---"
gzip -c "$DATA_DIR/packets.pcap" > "$DATA_DIR/packets.pcap.gz"
$CLICKHOUSE_LOCAL -q "
SELECT count(), countIf(ip_protocol = 'TCP') FROM file('$DATA_DIR/packets.pcap.gz', PCAP) FORMAT TSV"

echo "--- from a stream: an error of the input buffer is reported as is ---"
head -c 200 "$DATA_DIR/packets.pcap.gz" > "$DATA_DIR/broken.pcap.gz"
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$DATA_DIR/broken.pcap.gz', PCAP) FORMAT TSV" 2>&1 | grep -o -m1 'CANNOT_DECOMPRESS'

echo "--- schema inference via file() extension is PCAP-explicit only (count) ---"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/packets.pcap', PCAP)"

echo "--- format is inferred from the .pcap and .pcapng extensions ---"
for f in packets.pcap packets.pcapng; do
    $CLICKHOUSE_LOCAL -q "SELECT '$f', count(), countIf(ip_protocol = 'TCP') FROM file('$DATA_DIR/$f') FORMAT TSV"
done
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$DATA_DIR/packets.pcapng') FORMAT TSV" | cut -f1 | head -n 2

echo "--- payload covers only TCP and UDP: an ICMP packet has an empty payload ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, ip_protocol, payload_length, payload
FROM file('$DATA_DIR/packets.pcap', PCAP)
WHERE ip_protocol = 'ICMP' FORMAT TSV"

echo "--- raw IP link types: the IP header is the outermost layer ---"
for f in raw_ip.pcap ipv4.pcap ipv6.pcap; do
    $CLICKHOUSE_LOCAL -q "
    SELECT '$f', number, link_type, protocols, eth_src, eth_type, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port, payload_length
    FROM file('$DATA_DIR/$f', PCAP)
    ORDER BY number FORMAT TSV"
done

echo "--- BSD loopback and Linux cooked capture link types are dissected ---"
for f in null.pcap sll.pcap; do
    $CLICKHOUSE_LOCAL -q "
    SELECT '$f', number, link_type, protocols, eth_src, eth_type, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port, payload_length
    FROM file('$DATA_DIR/$f', PCAP)
    ORDER BY number FORMAT TSV"
done

echo "--- BSD loopback, Linux cooked capture and PPI frames cut inside the IP header keep their outer headers ---"
for f in truncated_null.pcap truncated_sll.pcap ppi.pcap; do
    $CLICKHOUSE_LOCAL -q "
    SELECT '$f', number, capture_length, original_length, link_type, protocols, eth_src, eth_type, ip_version, ip_protocol, length(raw)
    FROM file('$DATA_DIR/$f', PCAP)
    ORDER BY number FORMAT TSV"
done

echo "--- a link type that is not dissected yields rows with the capture metadata only ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, link_type, protocols, capture_length, length(raw), ip_version, ip_protocol, src_port
FROM file('$DATA_DIR/sll2.pcap', PCAP) FORMAT TSV"

echo "--- IEEE 802.3 frames: LLC, and IP behind SNAP ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, link_type, protocols, eth_src, eth_dst, eth_type, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port
FROM file('$DATA_DIR/dot3.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- end-of-options padding: the headers are dissected, the payload starts after the padding ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, ip_version, src_addr, dst_addr, ip_protocol, src_port, dst_port, payload_length, payload
FROM file('$DATA_DIR/options.pcap', PCAP)
WHERE ip_version = 4
ORDER BY number FORMAT TSV"

echo "--- non-first IPv6 fragment: the extension-header chain stops at the Fragment header ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, ip_protocol, src_port, dst_port, payload_length
FROM file('$DATA_DIR/options.pcap', PCAP)
WHERE ip_version = 6 FORMAT TSV"

echo "--- first IPv4 and IPv6 fragments: the transport header is decoded ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, protocols, ip_version, ip_protocol, src_port, dst_port, tcp_flags, tcp_seq, payload_length, payload
FROM file('$DATA_DIR/fragments.pcap', PCAP)
ORDER BY number FORMAT TSV"

echo "--- explicit structure: the columns are converted to the requested types ---"
$CLICKHOUSE_LOCAL -q "
SELECT number, toTypeName(number), dst_port, toTypeName(dst_port)
FROM file('$DATA_DIR/packets.pcap', PCAP, 'number UInt32, dst_port Nullable(String)')
ORDER BY number FORMAT TSV"
$CLICKHOUSE_LOCAL -q "
SELECT * FROM file('$DATA_DIR/packets.pcap', PCAP, 'no_such_column UInt64')" 2>&1 | grep -o -m1 'THERE_IS_NO_COLUMN'
$CLICKHOUSE_LOCAL -q "
SELECT * FROM file('$DATA_DIR/packets.pcap', PCAP, 'src_addr UInt8')" 2>&1 | grep -o -m1 'while converting column .src_addr. from type Nullable(IPv6) to type UInt8'
