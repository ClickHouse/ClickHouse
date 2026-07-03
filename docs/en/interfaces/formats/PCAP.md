---
alias: []
description: 'Documentation for the PCAP format'
input_format: true
keywords: ['PCAP', 'pcapng', 'packet capture']
output_format: false
slug: /interfaces/formats/PCAP
title: 'PCAP'
doc_type: 'reference'
---

| Input | Output  | Alias |
|-------|---------|-------|
| ✔     | ✗       |       |

## Description {#description}

The `PCAP` format reads network packet capture files - both the classic
`pcap` format and the newer `pcapng` format - and produces one row per packet.
Each packet is dissected up to the transport layer (L2-L4), so that Ethernet,
IPv4/IPv6 and TCP/UDP header fields are available as typed columns that can be
queried with SQL.

Parsing is performed with the `libtins` library, which reads the capture
container through `libpcap`. Only reading of capture files is supported; live
capture is not.

:::info
A capture file is a sequence of packet records. Each record stores the
capture timestamp, the number of bytes that were actually saved
(`capture_length`), the packet's true size on the wire (`original_length`),
and the captured bytes themselves. When a capture is taken with a *snapshot
length* smaller than the packets (for example `tcpdump -s 96`), only the first
bytes of each packet are saved, so `capture_length` is smaller than
`original_length`. For a full capture the two are equal.
:::

:::note
Capture files are read with microsecond timestamp precision. The `timestamp`
column has a nanosecond-scale type (`DateTime64(9)`) for future compatibility,
but nanosecond-resolution captures are currently truncated to microseconds.
:::

## Columns {#columns}

The `PCAP` format produces the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `number` | `UInt64` | 1-based index of the packet within the capture |
| `timestamp` | `DateTime64(9)` | Capture time of the packet. The column type has nanosecond scale, but capture files are read with microsecond precision, so the value is currently truncated to microseconds even for nanosecond captures |
| `capture_length` | `UInt32` | Number of bytes saved in the file for this packet; equals `length(raw)` |
| `original_length` | `UInt32` | Size of the packet on the wire; `>= capture_length` |
| `link_type` | `LowCardinality(String)` | Link-layer type of the outermost layer, for example `ETHERNET_II` |
| `protocols` | `Array(LowCardinality(String))` | Ordered list of protocols in the packet, from the link layer inwards, for example `['ETHERNET_II', 'IP', 'TCP']` |
| `eth_src` | `Nullable(String)` | Source MAC address, or `NULL` for non-Ethernet packets |
| `eth_dst` | `Nullable(String)` | Destination MAC address |
| `eth_type` | `LowCardinality(String)` | Protocol carried by the Ethernet frame, for example `IP` |
| `vlan_id` | `Nullable(UInt16)` | 802.1Q VLAN identifier, if present |
| `ip_version` | `Nullable(UInt8)` | `4` or `6`, or `NULL` for non-IP packets |
| `src_addr` | `Nullable(IPv6)` | Source IP address; IPv4 addresses are mapped to IPv6 (`::ffff:a.b.c.d`) |
| `dst_addr` | `Nullable(IPv6)` | Destination IP address |
| `ip_protocol` | `LowCardinality(String)` | Transport protocol, for example `TCP`, `UDP` or `ICMP` |
| `ip_ttl` | `Nullable(UInt16)` | IPv4 TTL or IPv6 hop limit |
| `src_port` | `Nullable(UInt16)` | TCP/UDP source port |
| `dst_port` | `Nullable(UInt16)` | TCP/UDP destination port |
| `tcp_flags` | `Nullable(String)` | Comma-separated TCP flags, for example `ACK,PSH` |
| `tcp_seq` | `Nullable(UInt32)` | TCP sequence number |
| `tcp_ack` | `Nullable(UInt32)` | TCP acknowledgement number |
| `payload_length` | `UInt32` | Number of bytes in the transport-layer payload |
| `payload` | `String` | Transport-layer payload bytes |
| `raw` | `String` | The full captured frame, starting at the link layer |

All layer-specific columns are `Nullable`, so packets that do not contain a
given layer (for example a non-IP packet, or a UDP packet that has no TCP
fields) still produce a row.

The transport-layer payload is a suffix of the raw frame, so its position in
`raw` is `capture_length - payload_length`, and
`substring(raw, capture_length - payload_length + 1) = payload`.

## Example usage {#example-usage}

Count packets by transport protocol:

```sql title="Query"
SELECT
    ip_protocol,
    count() AS c
FROM file('capture.pcap', PCAP)
GROUP BY ip_protocol
ORDER BY c DESC
```

```text title="Response"
┌─ip_protocol─┬──────c─┐
│ TCP         │ 268050 │
│ UDP         │ 189389 │
│ ICMP        │  13798 │
└─────────────┴────────┘
```

Find the top TCP destination ports:

```sql title="Query"
SELECT
    dst_port,
    count() AS c
FROM file('capture.pcap', PCAP)
WHERE ip_protocol = 'TCP'
GROUP BY dst_port
ORDER BY c DESC
LIMIT 5
```

Inspect the protocol stack of the first few packets:

```sql title="Query"
SELECT
    number,
    protocols,
    src_addr,
    dst_addr,
    src_port,
    dst_port
FROM file('capture.pcap', PCAP)
LIMIT 3
```

```text title="Response"
┌─number─┬─protocols──────────────────────────┬─src_addr───────────┬─dst_addr───────────┬─src_port─┬─dst_port─┐
│      1 │ ['ETHERNET_II','IP','TCP','RAW']   │ ::ffff:81.95.182.31│ ::ffff:10.0.0.46   │    40890 │    44850 │
│      2 │ ['ETHERNET_II','IP','TCP']         │ ::ffff:10.0.0.46   │ ::ffff:81.95.182.31│    44850 │    40890 │
│      3 │ ['ETHERNET_II','IP','TCP','RAW']   │ ::ffff:10.0.0.46   │ ::ffff:78.47.102.65│    60808 │    51413 │
└────────┴────────────────────────────────────┴────────────────────┴────────────────────┴──────────┴──────────┘
```

## Format settings {#format-settings}

The `PCAP` format currently has no format-specific settings.
