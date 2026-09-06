#include <Processors/Formats/Impl/PCAPBlockInputFormat.h>

#if USE_PCAP

#include <Formats/FormatFactory.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnUnique.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/copyData.h>
#include <Common/ErrnoException.h>

#include <tins/tins.h>
#include <tins/sniffer.h>
#include <tins/packet.h>
#include <tins/pdu.h>
#include <tins/ethernetII.h>
#include <tins/ip.h>
#include <tins/ipv6.h>
#include <tins/tcp.h>
#include <tins/udp.h>
#include <tins/dot1q.h>
#include <tins/utils/pdu_utils.h>
#include <tins/detail/pdu_helpers.h>

#include <pcap/pcap.h>

#include <array>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_SEEK_THROUGH_FILE;
}

/// One row per packet; up to this many packets per Chunk.
static constexpr size_t max_rows_per_chunk = 65536;

enum PcapColumn
{
    COL_NUMBER,
    COL_TIMESTAMP,
    COL_CAPTURE_LENGTH,
    COL_ORIGINAL_LENGTH,
    COL_LINK_TYPE,
    COL_PROTOCOLS,

    COL_ETH_SRC,
    COL_ETH_DST,
    COL_ETH_TYPE,
    COL_VLAN_ID,

    COL_IP_VERSION,
    COL_SRC_ADDR,
    COL_DST_ADDR,
    COL_IP_PROTOCOL,
    COL_IP_TTL,

    COL_SRC_PORT,
    COL_DST_PORT,

    COL_TCP_FLAGS,
    COL_TCP_SEQ,
    COL_TCP_ACK,

    COL_PAYLOAD_LENGTH,
    COL_PAYLOAD,
    COL_RAW,

    COL_COUNT,
};

static DataTypePtr nullable(DataTypePtr t)
{
    return std::make_shared<DataTypeNullable>(std::move(t));
}

static DataTypePtr lowCardinalityString()
{
    return std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
}

static NamesAndTypesList getHeaderForPCAP()
{
    NamesAndTypes cols(COL_COUNT);
    cols[COL_NUMBER] = {"number", std::make_shared<DataTypeUInt64>()};
    /// Nanosecond precision covers both microsecond and nanosecond captures.
    cols[COL_TIMESTAMP] = {"timestamp", std::make_shared<DataTypeDateTime64>(9)};
    cols[COL_CAPTURE_LENGTH] = {"capture_length", std::make_shared<DataTypeUInt32>()};
    cols[COL_ORIGINAL_LENGTH] = {"original_length", std::make_shared<DataTypeUInt32>()};
    cols[COL_LINK_TYPE] = {"link_type", lowCardinalityString()};
    cols[COL_PROTOCOLS] = {"protocols", std::make_shared<DataTypeArray>(lowCardinalityString())};

    cols[COL_ETH_SRC] = {"eth_src", nullable(std::make_shared<DataTypeString>())};
    cols[COL_ETH_DST] = {"eth_dst", nullable(std::make_shared<DataTypeString>())};
    cols[COL_ETH_TYPE] = {"eth_type", lowCardinalityString()};
    cols[COL_VLAN_ID] = {"vlan_id", nullable(std::make_shared<DataTypeUInt16>())};

    cols[COL_IP_VERSION] = {"ip_version", nullable(std::make_shared<DataTypeUInt8>())};
    cols[COL_SRC_ADDR] = {"src_addr", nullable(std::make_shared<DataTypeIPv6>())};
    cols[COL_DST_ADDR] = {"dst_addr", nullable(std::make_shared<DataTypeIPv6>())};
    cols[COL_IP_PROTOCOL] = {"ip_protocol", lowCardinalityString()};
    cols[COL_IP_TTL] = {"ip_ttl", nullable(std::make_shared<DataTypeUInt16>())};

    cols[COL_SRC_PORT] = {"src_port", nullable(std::make_shared<DataTypeUInt16>())};
    cols[COL_DST_PORT] = {"dst_port", nullable(std::make_shared<DataTypeUInt16>())};

    cols[COL_TCP_FLAGS] = {"tcp_flags", nullable(std::make_shared<DataTypeString>())};
    cols[COL_TCP_SEQ] = {"tcp_seq", nullable(std::make_shared<DataTypeUInt32>())};
    cols[COL_TCP_ACK] = {"tcp_ack", nullable(std::make_shared<DataTypeUInt32>())};

    cols[COL_PAYLOAD_LENGTH] = {"payload_length", std::make_shared<DataTypeUInt32>()};
    cols[COL_PAYLOAD] = {"payload", std::make_shared<DataTypeString>()};
    cols[COL_RAW] = {"raw", std::make_shared<DataTypeString>()};

    return NamesAndTypesList(cols.begin(), cols.end());
}

static const std::unordered_map<std::string, size_t> & getColumnNameToIdx()
{
    static std::once_flag once;
    static std::unordered_map<std::string, size_t> name_to_idx;
    std::call_once(once, [&]
    {
        size_t i = 0;
        for (const auto & c : getHeaderForPCAP())
        {
            name_to_idx.emplace(c.name, i);
            ++i;
        }
    });
    return name_to_idx;
}

PCAPBlockInputFormat::PCAPBlockInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_)
{
}

PCAPBlockInputFormat::~PCAPBlockInputFormat()
{
    closeFile();
}

void PCAPBlockInputFormat::closeFile()
{
    sniffer.reset();
    if (capture_file != nullptr)
    {
        /// The stream is only ever read through, so there is nothing to flush and nothing that can fail.
        [[maybe_unused]] int rc = fclose(capture_file);
        capture_file = nullptr;
    }
}

void PCAPBlockInputFormat::initializeIfNeeded()
{
    if (initialized)
        return;
    initialized = true;

    Tins::SnifferConfiguration config;

    ReadBuffer & input = *in;

    /// If it's a local file at offset 0, let libpcap open it directly.
    if (auto * file_in = dynamic_cast<ReadBufferFromFileBase *>(&input))
    {
        size_t offset = 0;
        if (file_in->isRegularLocalFile(&offset) && offset == 0)
        {
            sniffer = std::make_unique<Tins::FileSniffer>(file_in->getFileName(), config);
            return;
        }
    }

    /// Otherwise copy the whole stream into a temporary file: `libpcap` reads a capture only
    /// from a path or from a `FILE *`, and it seeks, while the input stream is not seekable.
    capture_file = tmpfile();
    if (capture_file == nullptr)
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot create a temporary file to read a PCAP capture");

    {
        WriteBufferFromFileDescriptor out(fileno(capture_file));
        /// The static analyzer models a failed `dynamic_cast` above as if the operand pointer
        /// itself could be null, but `in` is never null here.
        copyData(input, out, is_stopped); /// NOLINT(clang-analyzer-core.NonNullParamChecker)
        out.finalize();
    }

    /// The stream is only partially copied if the query has been cancelled;
    /// do not open a sniffer over a truncated capture.
    if (is_stopped)
        return;

    if (fseek(capture_file, 0, SEEK_SET) != 0)
        throw ErrnoException(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Cannot rewind the temporary file with a PCAP capture");

    sniffer = std::make_unique<Tins::FileSniffer>(capture_file, config);

    /// `pcap_fopen_offline` succeeded, and `pcap_close` (run by the sniffer's destructor)
    /// closes the underlying `FILE *` itself, so ownership has been transferred to `libpcap`.
    /// On failure the `FILE *` stays ours and `closeFile` releases it.
    capture_file = nullptr;
}

namespace
{

/// Writes the 16 address bytes of an IPv4-mapped IPv6 address (::ffff:a.b.c.d).
void writeIPv4Mapped(UInt32 ipv4_be, IPv6 & out)
{
    auto * buf = reinterpret_cast<UInt8 *>(&out.toUnderType());
    memset(buf, 0, 10);
    buf[10] = 0xff;
    buf[11] = 0xff;
    /// ipv4_be is already in network byte order.
    memcpy(buf + 12, &ipv4_be, 4);
}

void writeIPv6(const Tins::IPv6Address & addr, IPv6 & out)
{
    auto * buf = reinterpret_cast<UInt8 *>(&out.toUnderType());
    size_t i = 0;
    for (auto byte : addr)
        buf[i++] = byte;
}

/// Name of the IANA protocol number found in the IPv4 `protocol` or IPv6 `next header` field.
/// Numbers without a well-known name are rendered in decimal.
String ipProtocolName(UInt8 protocol)
{
    switch (protocol)
    {
        case 0: return "HOPOPT";
        case 1: return "ICMP";
        case 2: return "IGMP";
        case 4: return "IPv4";
        case 6: return "TCP";
        case 17: return "UDP";
        case 41: return "IPv6";
        case 43: return "IPv6-Route";
        case 44: return "IPv6-Frag";
        case 47: return "GRE";
        case 50: return "ESP";
        case 51: return "AH";
        case 58: return "ICMPv6";
        case 59: return "IPv6-NoNxt";
        case 60: return "IPv6-Opts";
        case 89: return "OSPF";
        case 103: return "PIM";
        case 112: return "VRRP";
        case 115: return "L2TP";
        case 132: return "SCTP";
        case 136: return "UDPLite";
        case 137: return "MPLS-in-IP";
        default: return std::to_string(protocol);
    }
}

String formatTCPFlags(const Tins::TCP & tcp)
{
    static constexpr std::array<std::pair<Tins::TCP::Flags, const char *>, 8> names{{
        {Tins::TCP::SYN, "SYN"},
        {Tins::TCP::ACK, "ACK"},
        {Tins::TCP::FIN, "FIN"},
        {Tins::TCP::RST, "RST"},
        {Tins::TCP::PSH, "PSH"},
        {Tins::TCP::URG, "URG"},
        {Tins::TCP::ECE, "ECE"},
        {Tins::TCP::CWR, "CWR"},
    }};

    String result;
    for (const auto & [flag, name] : names)
    {
        if (tcp.get_flag(flag))
        {
            if (!result.empty())
                result += ",";
            result += name;
        }
    }
    return result;
}

}

Chunk PCAPBlockInputFormat::read()
{
    initializeIfNeeded();

    const auto & header = getPort().getHeader();
    const auto & column_name_to_idx = getColumnNameToIdx();
    std::array<bool, COL_COUNT> need{};
    for (const std::string & name : header.getNames())
        need[column_name_to_idx.at(name)] = true;

    /// LowCardinality(String) columns are built directly via insertData.
    static const auto lc_string_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    auto make_lc = [] { return lc_string_type->createColumn(); };

    auto col_number = ColumnUInt64::create();
    auto col_timestamp = ColumnDateTime64::create(0, 9);
    auto col_capture_length = ColumnUInt32::create();
    auto col_original_length = ColumnUInt32::create();
    auto col_link_type = make_lc();

    auto col_protocols_data = make_lc();
    auto col_protocols_offsets = ColumnArray::ColumnOffsets::create();

    auto make_nullable_string = [] { return std::make_pair(ColumnString::create(), ColumnUInt8::create()); };
    auto [col_eth_src, col_eth_src_null] = make_nullable_string();
    auto [col_eth_dst, col_eth_dst_null] = make_nullable_string();
    auto col_eth_type = make_lc();
    auto col_vlan_id = ColumnUInt16::create();
    auto col_vlan_id_null = ColumnUInt8::create();

    auto col_ip_version = ColumnUInt8::create();
    auto col_ip_version_null = ColumnUInt8::create();
    auto col_src_addr = ColumnIPv6::create();
    auto col_src_addr_null = ColumnUInt8::create();
    auto col_dst_addr = ColumnIPv6::create();
    auto col_dst_addr_null = ColumnUInt8::create();
    auto col_ip_protocol = make_lc();
    auto col_ip_ttl = ColumnUInt16::create();
    auto col_ip_ttl_null = ColumnUInt8::create();

    auto col_src_port = ColumnUInt16::create();
    auto col_src_port_null = ColumnUInt8::create();
    auto col_dst_port = ColumnUInt16::create();
    auto col_dst_port_null = ColumnUInt8::create();

    auto [col_tcp_flags, col_tcp_flags_null] = make_nullable_string();
    auto col_tcp_seq = ColumnUInt32::create();
    auto col_tcp_seq_null = ColumnUInt8::create();
    auto col_tcp_ack = ColumnUInt32::create();
    auto col_tcp_ack_null = ColumnUInt8::create();

    auto col_payload_length = ColumnUInt32::create();
    auto col_payload = ColumnString::create();
    auto col_raw = ColumnString::create();

    if (sniffer == nullptr)
        return {};

    pcap_t * handle = sniffer->get_pcap_handle();
    const int dlt = sniffer->link_type();

    size_t num_rows = 0;
    size_t bytes_read = 0;

    while (num_rows < max_rows_per_chunk)
    {
        if (is_stopped)
            break;

        /// Read the raw packet with its original pcap header (caplen, len, ts).
        pcap_pkthdr * pkthdr = nullptr;
        const u_char * data = nullptr;
        int res = pcap_next_ex(handle, &pkthdr, &data);
        if (res == -2 || res == 0) /// -2: end of savefile, 0: timeout (not applicable offline).
            break;
        if (res < 0)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Failed to read packet from PCAP: {}", pcap_geterr(handle));

        const UInt32 caplen = pkthdr->caplen;
        const UInt32 wire_len = pkthdr->len;

        /// Dissect the raw bytes into a PDU chain (rawpdu_on_no_match=true keeps
        /// undissected tails as a RAW pdu).
        std::unique_ptr<Tins::PDU> pdu_owner(
            Tins::Internals::pdu_from_dlt_flag(dlt, data, caplen, /*rawpdu_on_no_match=*/ true));
        Tins::PDU * pdu = pdu_owner.get();
        if (pdu == nullptr)
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED,
                "Cannot dissect packet {} in PCAP input", packet_number + 1);

        ++num_rows;
        ++packet_number;

        bytes_read += caplen;

        if (need[COL_NUMBER])
            col_number->insertValue(packet_number);

        if (need[COL_TIMESTAMP])
        {
            /// FileSniffer opens with microsecond precision, so tv_usec is microseconds.
            Int64 nanos = static_cast<Int64>(pkthdr->ts.tv_sec) * 1'000'000'000
                + static_cast<Int64>(pkthdr->ts.tv_usec) * 1'000;
            col_timestamp->insertValue(nanos);
        }

        if (need[COL_CAPTURE_LENGTH])
            col_capture_length->insertValue(caplen);
        if (need[COL_ORIGINAL_LENGTH])
            col_original_length->insertValue(wire_len);

        if (need[COL_LINK_TYPE])
        {
            /// The outermost PDU type is the link layer.
            String name = Tins::Utils::to_string(pdu->pdu_type());
            col_link_type->insertData(name.data(), name.size());
        }

        if (need[COL_PROTOCOLS])
        {
            for (const Tins::PDU * p = pdu; p != nullptr; p = p->inner_pdu())
            {
                String name = Tins::Utils::to_string(p->pdu_type());
                col_protocols_data->insertData(name.data(), name.size());
            }
            col_protocols_offsets->insertValue(col_protocols_data->size());
        }

        /// Ethernet.
        auto insert_null_string = [](ColumnString & col, ColumnUInt8 & nullmap) { col.insertDefault(); nullmap.insertValue(1); };
        if (auto * eth = pdu->find_pdu<Tins::EthernetII>())
        {
            if (need[COL_ETH_SRC]) { String s = eth->src_addr().to_string(); col_eth_src->insertData(s.data(), s.size()); col_eth_src_null->insertValue(0); }
            if (need[COL_ETH_DST]) { String s = eth->dst_addr().to_string(); col_eth_dst->insertData(s.data(), s.size()); col_eth_dst_null->insertValue(0); }
        }
        else
        {
            if (need[COL_ETH_SRC]) insert_null_string(*col_eth_src, *col_eth_src_null);
            if (need[COL_ETH_DST]) insert_null_string(*col_eth_dst, *col_eth_dst_null);
        }

        if (need[COL_VLAN_ID])
        {
            if (const auto * vlan = pdu->find_pdu<Tins::Dot1Q>())
            {
                col_vlan_id->insertValue(static_cast<UInt16>(vlan->id()));
                col_vlan_id_null->insertValue(0);
            }
            else
            {
                col_vlan_id->insertDefault();
                col_vlan_id_null->insertValue(1);
            }
        }

        /// Protocol carried by the Ethernet frame, or empty for a non-Ethernet packet.
        /// 802.1Q tags are unwrapped, so a VLAN-tagged IPv4 frame reports `IP` and not
        /// `DOT1Q`; the tag itself is exposed separately as `vlan_id`.
        if (need[COL_ETH_TYPE])
        {
            String name;
            if (const auto * eth = pdu->find_pdu<Tins::EthernetII>())
            {
                const Tins::PDU * inner = eth->inner_pdu();
                while (inner != nullptr && inner->pdu_type() == Tins::PDU::DOT1Q)
                    inner = inner->inner_pdu();
                if (inner != nullptr)
                    name = Tins::Utils::to_string(inner->pdu_type());
            }
            col_eth_type->insertData(name.data(), name.size());
        }

        /// IP layer (v4 or v6).
        const auto * ipv4 = pdu->find_pdu<Tins::IP>();
        const auto * ipv6 = pdu->find_pdu<Tins::IPv6>();

        if (need[COL_IP_VERSION])
        {
            if (ipv4) { col_ip_version->insertValue(4); col_ip_version_null->insertValue(0); }
            else if (ipv6) { col_ip_version->insertValue(6); col_ip_version_null->insertValue(0); }
            else { col_ip_version->insertDefault(); col_ip_version_null->insertValue(1); }
        }

        if (need[COL_SRC_ADDR] || need[COL_DST_ADDR])
        {
            IPv6 src{};
            IPv6 dst{};
            bool has = true;
            if (ipv4)
            {
                writeIPv4Mapped(static_cast<UInt32>(ipv4->src_addr()), src);
                writeIPv4Mapped(static_cast<UInt32>(ipv4->dst_addr()), dst);
            }
            else if (ipv6)
            {
                writeIPv6(ipv6->src_addr(), src);
                writeIPv6(ipv6->dst_addr(), dst);
            }
            else
                has = false;

            if (need[COL_SRC_ADDR]) { col_src_addr->insertValue(src); col_src_addr_null->insertValue(has ? 0 : 1); }
            if (need[COL_DST_ADDR]) { col_dst_addr->insertValue(dst); col_dst_addr_null->insertValue(has ? 0 : 1); }
        }

        if (need[COL_IP_TTL])
        {
            if (ipv4) { col_ip_ttl->insertValue(ipv4->ttl()); col_ip_ttl_null->insertValue(0); }
            else if (ipv6) { col_ip_ttl->insertValue(ipv6->hop_limit()); col_ip_ttl_null->insertValue(0); }
            else { col_ip_ttl->insertDefault(); col_ip_ttl_null->insertValue(1); }
        }

        /// Transport fields describe the same outer IP layer as the address fields.
        Tins::PDU * ip_inner = ipv4 ? ipv4->inner_pdu() : (ipv6 ? ipv6->inner_pdu() : nullptr);
        auto * tcp = dynamic_cast<Tins::TCP *>(ip_inner);
        auto * udp = dynamic_cast<Tins::UDP *>(ip_inner);

        /// Taken from the IP header itself (the IPv4 `protocol` field or the IPv6 `next header`
        /// field), so it stays correct when the transport header was not captured (a truncated
        /// snapshot) or is not decodable (a non-first IPv4 fragment).
        if (need[COL_IP_PROTOCOL])
        {
            String name;
            if (ipv4)
                name = ipProtocolName(ipv4->protocol());
            else if (ipv6)
                name = ipProtocolName(ipv6->next_header());
            col_ip_protocol->insertData(name.data(), name.size());
        }

        if (need[COL_SRC_PORT])
        {
            if (tcp) { col_src_port->insertValue(tcp->sport()); col_src_port_null->insertValue(0); }
            else if (udp) { col_src_port->insertValue(udp->sport()); col_src_port_null->insertValue(0); }
            else { col_src_port->insertDefault(); col_src_port_null->insertValue(1); }
        }
        if (need[COL_DST_PORT])
        {
            if (tcp) { col_dst_port->insertValue(tcp->dport()); col_dst_port_null->insertValue(0); }
            else if (udp) { col_dst_port->insertValue(udp->dport()); col_dst_port_null->insertValue(0); }
            else { col_dst_port->insertDefault(); col_dst_port_null->insertValue(1); }
        }

        if (need[COL_TCP_FLAGS])
        {
            if (tcp) { String f = formatTCPFlags(*tcp); col_tcp_flags->insertData(f.data(), f.size()); col_tcp_flags_null->insertValue(0); }
            else insert_null_string(*col_tcp_flags, *col_tcp_flags_null);
        }
        if (need[COL_TCP_SEQ])
        {
            if (tcp) { col_tcp_seq->insertValue(tcp->seq()); col_tcp_seq_null->insertValue(0); }
            else { col_tcp_seq->insertDefault(); col_tcp_seq_null->insertValue(1); }
        }
        if (need[COL_TCP_ACK])
        {
            if (tcp) { col_tcp_ack->insertValue(tcp->ack_seq()); col_tcp_ack_null->insertValue(0); }
            else { col_tcp_ack->insertDefault(); col_tcp_ack_null->insertValue(1); }
        }

        /// Payload = bytes carried by the innermost recognized transport layer.
        if (need[COL_PAYLOAD] || need[COL_PAYLOAD_LENGTH])
        {
            std::string_view payload;
            Tins::PDU * inner = nullptr;
            if (tcp) inner = tcp->inner_pdu();
            else if (udp) inner = udp->inner_pdu();

            Tins::PDU::serialization_type payload_bytes;
            if (inner != nullptr)
            {
                payload_bytes = inner->serialize();
                payload = std::string_view(reinterpret_cast<const char *>(payload_bytes.data()), payload_bytes.size());
            }

            if (need[COL_PAYLOAD_LENGTH])
                col_payload_length->insertValue(static_cast<UInt32>(payload.size()));
            if (need[COL_PAYLOAD])
                col_payload->insertData(payload.data(), payload.size());
        }

        if (need[COL_RAW])
            col_raw->insertData(reinterpret_cast<const char *>(data), caplen);
    }

    approx_bytes_read_for_chunk = bytes_read;

    if (num_rows == 0)
        return {};

    auto make_nullable = [](MutableColumnPtr col, MutableColumnPtr nullmap) -> ColumnPtr
    {
        return ColumnNullable::create(std::move(col), std::move(nullmap));
    };

    /// Every column is moved exactly once here, outside of any loop.
    std::array<ColumnPtr, COL_COUNT> built;
    built[COL_NUMBER] = std::move(col_number);
    built[COL_TIMESTAMP] = std::move(col_timestamp);
    built[COL_CAPTURE_LENGTH] = std::move(col_capture_length);
    built[COL_ORIGINAL_LENGTH] = std::move(col_original_length);
    built[COL_LINK_TYPE] = std::move(col_link_type);
    built[COL_PROTOCOLS] = ColumnArray::create(std::move(col_protocols_data), std::move(col_protocols_offsets));
    built[COL_ETH_SRC] = make_nullable(std::move(col_eth_src), std::move(col_eth_src_null));
    built[COL_ETH_DST] = make_nullable(std::move(col_eth_dst), std::move(col_eth_dst_null));
    built[COL_ETH_TYPE] = std::move(col_eth_type);
    built[COL_VLAN_ID] = make_nullable(std::move(col_vlan_id), std::move(col_vlan_id_null));
    built[COL_IP_VERSION] = make_nullable(std::move(col_ip_version), std::move(col_ip_version_null));
    built[COL_SRC_ADDR] = make_nullable(std::move(col_src_addr), std::move(col_src_addr_null));
    built[COL_DST_ADDR] = make_nullable(std::move(col_dst_addr), std::move(col_dst_addr_null));
    built[COL_IP_PROTOCOL] = std::move(col_ip_protocol);
    built[COL_IP_TTL] = make_nullable(std::move(col_ip_ttl), std::move(col_ip_ttl_null));
    built[COL_SRC_PORT] = make_nullable(std::move(col_src_port), std::move(col_src_port_null));
    built[COL_DST_PORT] = make_nullable(std::move(col_dst_port), std::move(col_dst_port_null));
    built[COL_TCP_FLAGS] = make_nullable(std::move(col_tcp_flags), std::move(col_tcp_flags_null));
    built[COL_TCP_SEQ] = make_nullable(std::move(col_tcp_seq), std::move(col_tcp_seq_null));
    built[COL_TCP_ACK] = make_nullable(std::move(col_tcp_ack), std::move(col_tcp_ack_null));
    built[COL_PAYLOAD_LENGTH] = std::move(col_payload_length);
    built[COL_PAYLOAD] = std::move(col_payload);
    built[COL_RAW] = std::move(col_raw);

    Columns cols;
    cols.reserve(header.columns());
    for (const std::string & name : header.getNames())
    {
        size_t idx = column_name_to_idx.at(name);
        if (idx >= COL_COUNT || !built[idx])
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Unexpected PCAP column index");
        cols.push_back(built[idx]);
    }

    return Chunk(std::move(cols), num_rows);
}

void PCAPBlockInputFormat::resetParser()
{
    closeFile();
    initialized = false;
    packet_number = 0;
    approx_bytes_read_for_chunk = 0;

    IInputFormat::resetParser();
}

PCAPSchemaReader::PCAPSchemaReader(ReadBuffer & in_)
    : ISchemaReader(in_)
{
}

NamesAndTypesList PCAPSchemaReader::readSchema()
{
    return getHeaderForPCAP();
}

void registerInputFormatPCAP(FormatFactory & factory);
void registerInputFormatPCAP(FormatFactory & factory)
{
    factory.registerRandomAccessInputFormat(
        "PCAP",
        [](ReadBuffer & buf,
           const Block & sample,
           const FormatSettings & settings,
           const ReadSettings &,
           bool /* is_remote_fs */,
           FormatParserSharedResourcesPtr,
           FormatFilterInfoPtr) -> InputFormatPtr
        {
            return std::make_shared<PCAPBlockInputFormat>(buf, std::make_shared<const Block>(sample), settings);
        });
    factory.markFormatSupportsSubsetOfColumns("PCAP");

    factory.setDocumentation("PCAP", Documentation{
        .description = R"DOCS_MD(
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
| `eth_type` | `LowCardinality(String)` | Protocol carried by the Ethernet frame, for example `IP`. 802.1Q tags are unwrapped, so a VLAN-tagged frame reports the encapsulated protocol and not `DOT1Q` |
| `vlan_id` | `Nullable(UInt16)` | 802.1Q VLAN identifier, if present |
| `ip_version` | `Nullable(UInt8)` | `4` or `6`, or `NULL` for non-IP packets |
| `src_addr` | `Nullable(IPv6)` | Source IP address; IPv4 addresses are mapped to IPv6 (`::ffff:a.b.c.d`) |
| `dst_addr` | `Nullable(IPv6)` | Destination IP address |
| `ip_protocol` | `LowCardinality(String)` | Protocol carried by the IP packet, taken from the IPv4 `protocol` field or the IPv6 `next header` field, for example `TCP`, `UDP` or `ICMP`. It is filled even when the transport header itself was not captured or is not decodable, in which case `src_port`, `dst_port` and the TCP columns stay `NULL`. Numbers without a well-known name are rendered in decimal. Empty for non-IP packets |
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

## Storing captures in a table {#storing-captures-in-a-table}

The `PCAP` format is read-only, so to keep captures around for repeated
analysis you typically ingest them into a `MergeTree` table. Create a table
whose columns match the ones you care about and `INSERT ... SELECT` from the
capture file:

```sql title="Create table and ingest"
CREATE TABLE packets
(
    timestamp       DateTime64(9),
    src_addr        Nullable(IPv6),
    dst_addr        Nullable(IPv6),
    ip_protocol     LowCardinality(String),
    src_port        Nullable(UInt16),
    dst_port        Nullable(UInt16),
    tcp_flags       Nullable(String),
    capture_length  UInt32,
    payload_length  UInt32,
    raw             String
)
ENGINE = MergeTree
ORDER BY (ip_protocol, timestamp);

INSERT INTO packets
SELECT
    timestamp,
    src_addr,
    dst_addr,
    ip_protocol,
    src_port,
    dst_port,
    tcp_flags,
    capture_length,
    payload_length,
    raw
FROM file('capture.pcap', PCAP);
```

You usually do not need to store both `raw` and `payload`. The payload is a
suffix of the raw frame, so it can always be recovered from `raw` together
with `capture_length` and `payload_length`:

```sql title="Recover the payload from raw"
SELECT substring(raw, (capture_length - payload_length) + 1) AS payload
FROM packets;
```

Storing only `raw` (plus the two lengths) avoids duplicating the packet bytes,
while still letting you reconstruct the payload on demand. If you never need
the link-layer/header bytes, you can do the opposite and store only `payload`.

The best `ORDER BY` key depends on your queries. Two common choices:

- For flow- or service-oriented analysis (grouping by protocol, port, or
  endpoint), lead with the low-cardinality, frequently-filtered columns, for
  example `ORDER BY (ip_protocol, timestamp)`. This clusters packets of the
  same protocol together and makes such filters and aggregations cheap.
- For time-series analysis (traffic over time, replaying a window of a
  capture), lead with `timestamp`, for example `ORDER BY (timestamp)`, which
  keeps chronologically close packets together and speeds up range scans over
  time.

Put the columns you filter on most often first, and remember that a
low-cardinality prefix (like `ip_protocol`) compresses and skips better than a
high-cardinality one. Note that most layer-specific columns (such as
`dst_port` or `src_addr`) are `Nullable`, and `MergeTree` rejects nullable
columns in the sorting key unless the `allow_nullable_key` setting is enabled;
prefer non-nullable columns like `ip_protocol` and `timestamp` in `ORDER BY`,
or cast/default the nullable ones (for example `assumeNotNull(dst_port)`) if
you need them in the key.

## Format settings {#format-settings}

The `PCAP` format currently has no format-specific settings.
)DOCS_MD"});
}

void registerPCAPSchemaReader(FormatFactory & factory);
void registerPCAPSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "PCAP",
        [](ReadBuffer & buf, const FormatSettings &)
        {
            return std::make_shared<PCAPSchemaReader>(buf);
        });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatPCAP(FormatFactory &);
void registerPCAPSchemaReader(FormatFactory &);
void registerInputFormatPCAP(FormatFactory &)
{
}
void registerPCAPSchemaReader(FormatFactory &)
{
}
}

#endif
