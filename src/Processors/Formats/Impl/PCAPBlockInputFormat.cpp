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
#include <IO/WriteBufferFromVector.h>
#include <IO/copyData.h>

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
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_READ_ALL_DATA;
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
    if (mem_file != nullptr)
    {
        fclose(mem_file);
        mem_file = nullptr;
    }
}

void PCAPBlockInputFormat::initializeIfNeeded()
{
    if (initialized)
        return;
    initialized = true;

    Tins::SnifferConfiguration config;

    /// If it's a local file at offset 0, let libpcap open it directly.
    if (auto * file_in = dynamic_cast<ReadBufferFromFileBase *>(in))
    {
        size_t offset = 0;
        if (file_in->isRegularLocalFile(&offset) && offset == 0)
        {
            sniffer = std::make_unique<Tins::FileSniffer>(file_in->getFileName(), config);
            return;
        }
    }

    /// Otherwise read the whole stream into memory and open it via fmemopen().
    {
        auto buf = WriteBufferFromVector<PODArray<char>>(file_contents);
        copyData(*in, buf, nullptr);
    }

    mem_file = fmemopen(file_contents.data(), file_contents.size(), "rb");
    if (mem_file == nullptr)
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Failed to open in-memory PCAP buffer");

    sniffer = std::make_unique<Tins::FileSniffer>(mem_file, config);
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

String formatTcpFlags(const Tins::TCP & tcp)
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

    pcap_t * handle = sniffer->get_pcap_handle();
    const int dlt = sniffer->link_type();

    size_t num_rows = 0;
    size_t bytes_read = 0;

    while (num_rows < max_rows_per_chunk)
    {
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
            continue;

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

        /// Ethernet type (name of the layer just under Ethernet, or empty).
        if (need[COL_ETH_TYPE])
        {
            String name;
            if (const auto * eth = pdu->find_pdu<Tins::EthernetII>(); eth && eth->inner_pdu())
                name = Tins::Utils::to_string(eth->inner_pdu()->pdu_type());
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

        /// Transport layer.
        auto * tcp = pdu->find_pdu<Tins::TCP>();
        auto * udp = pdu->find_pdu<Tins::UDP>();

        if (need[COL_IP_PROTOCOL])
        {
            String name;
            if (tcp) name = "TCP";
            else if (udp) name = "UDP";
            else if (ipv4 && ipv4->inner_pdu()) name = Tins::Utils::to_string(ipv4->inner_pdu()->pdu_type());
            else if (ipv6 && ipv6->inner_pdu()) name = Tins::Utils::to_string(ipv6->inner_pdu()->pdu_type());
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
            if (tcp) { String f = formatTcpFlags(*tcp); col_tcp_flags->insertData(f.data(), f.size()); col_tcp_flags_null->insertValue(0); }
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

    Columns cols;
    for (const std::string & name : header.getNames())
    {
        switch (column_name_to_idx.at(name))
        {
            case COL_NUMBER: cols.push_back(std::move(col_number)); break;
            case COL_TIMESTAMP: cols.push_back(std::move(col_timestamp)); break;
            case COL_CAPTURE_LENGTH: cols.push_back(std::move(col_capture_length)); break;
            case COL_ORIGINAL_LENGTH: cols.push_back(std::move(col_original_length)); break;
            case COL_LINK_TYPE:
                cols.push_back(std::move(col_link_type));
                break;
            case COL_PROTOCOLS:
                cols.push_back(ColumnArray::create(std::move(col_protocols_data), std::move(col_protocols_offsets)));
                break;
            case COL_ETH_SRC: cols.push_back(make_nullable(std::move(col_eth_src), std::move(col_eth_src_null))); break;
            case COL_ETH_DST: cols.push_back(make_nullable(std::move(col_eth_dst), std::move(col_eth_dst_null))); break;
            case COL_ETH_TYPE:
                cols.push_back(std::move(col_eth_type));
                break;
            case COL_VLAN_ID: cols.push_back(make_nullable(std::move(col_vlan_id), std::move(col_vlan_id_null))); break;
            case COL_IP_VERSION: cols.push_back(make_nullable(std::move(col_ip_version), std::move(col_ip_version_null))); break;
            case COL_SRC_ADDR: cols.push_back(make_nullable(std::move(col_src_addr), std::move(col_src_addr_null))); break;
            case COL_DST_ADDR: cols.push_back(make_nullable(std::move(col_dst_addr), std::move(col_dst_addr_null))); break;
            case COL_IP_PROTOCOL:
                cols.push_back(std::move(col_ip_protocol));
                break;
            case COL_IP_TTL: cols.push_back(make_nullable(std::move(col_ip_ttl), std::move(col_ip_ttl_null))); break;
            case COL_SRC_PORT: cols.push_back(make_nullable(std::move(col_src_port), std::move(col_src_port_null))); break;
            case COL_DST_PORT: cols.push_back(make_nullable(std::move(col_dst_port), std::move(col_dst_port_null))); break;
            case COL_TCP_FLAGS: cols.push_back(make_nullable(std::move(col_tcp_flags), std::move(col_tcp_flags_null))); break;
            case COL_TCP_SEQ: cols.push_back(make_nullable(std::move(col_tcp_seq), std::move(col_tcp_seq_null))); break;
            case COL_TCP_ACK: cols.push_back(make_nullable(std::move(col_tcp_ack), std::move(col_tcp_ack_null))); break;
            case COL_PAYLOAD_LENGTH: cols.push_back(std::move(col_payload_length)); break;
            case COL_PAYLOAD: cols.push_back(std::move(col_payload)); break;
            case COL_RAW: cols.push_back(std::move(col_raw)); break;
            default:
                throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Unexpected PCAP column index");
        }
    }

    return Chunk(std::move(cols), num_rows);
}

void PCAPBlockInputFormat::resetParser()
{
    closeFile();
    initialized = false;
    file_contents.clear();
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

The `PCAP` format reads packet capture files (both classic `pcap` and `pcapng`)
using `libtins` and `libpcap`. It produces one row per packet, with decoded
L2-L4 header fields (Ethernet, IPv4/IPv6, TCP/UDP), the ordered list of
protocols in the packet, the L4 payload, and the raw captured frame.
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
