#include <Processors/Formats/Impl/PCAPBlockInputFormat.h>

#if USE_PCAP

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowInputFormat.h>
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
#include <Formats/castColumnToRequestedType.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/ErrnoException.h>

#include <tins/tins.h>
#include <tins/sniffer.h>
#include <tins/packet.h>
#include <tins/pdu.h>
#include <tins/ethernetII.h>
#include <tins/dot3.h>
#include <tins/llc.h>
#include <tins/snap.h>
#include <tins/ip.h>
#include <tins/ipv6.h>
#include <tins/tcp.h>
#include <tins/udp.h>
#include <tins/dot1q.h>
#include <tins/loopback.h>
#include <tins/sll.h>
#include <tins/ppi.h>
#include <tins/utils/pdu_utils.h>
#include <tins/detail/pdu_helpers.h>

#include <pcap/pcap.h>

#include <array>
#include <bit>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int THERE_IS_NO_COLUMN;
}

/// One row per packet; up to this many packets per Chunk.

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

static const DataTypes & getColumnTypes()
{
    static const DataTypes types = getHeaderForPCAP().getTypes();
    return types;
}

/// The PCAP format has a fixed set of columns, so a name that is not one of them can not be read.
static size_t getColumnIdx(const std::string & name)
{
    const auto & name_to_idx = getColumnNameToIdx();
    auto it = name_to_idx.find(name);
    if (it == name_to_idx.end())
        throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", name);
    return it->second;
}

PCAPBlockInputFormat::PCAPBlockInputFormat(
    ReadBuffer & in_, SharedHeader header_, const FormatSettings & format_settings_, size_t max_block_size_rows_)
    : IInputFormat(std::move(header_), &in_), format_settings(format_settings_), max_block_size_rows(max_block_size_rows_)
{
    for (const std::string & name : getPort().getHeader().getNames())
        getColumnIdx(name);
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

    /// Otherwise let `libpcap` read the capture straight from the input buffer. `libpcap` reads
    /// a capture only through a path or a `FILE *`, but it reads it strictly sequentially, so a
    /// `FILE *` backed by the input buffer is enough and nothing has to be spooled anywhere.
    stream_cookie.in = &input;
    stream_cookie.exception = nullptr;

    cookie_io_functions_t functions{};
    functions.read = &PCAPBlockInputFormat::readFromInput;
    functions.close = &PCAPBlockInputFormat::closeInput;
    capture_file = fopencookie(&stream_cookie, "r", functions);
    if (capture_file == nullptr)
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open a stream to read a PCAP capture");

    try
    {
        sniffer = std::make_unique<Tins::FileSniffer>(capture_file, config);
    }
    catch (...)
    {
        /// `libpcap` reports a failed read of the header with its own message; the error
        /// thrown by the input buffer is the actual cause.
        rethrowInputException();
        throw;
    }

    /// `pcap_fopen_offline` succeeded, and `pcap_close` (run by the sniffer's destructor)
    /// closes the underlying `FILE *` itself, so ownership has been transferred to `libpcap`.
    /// On failure the `FILE *` stays ours and `closeFile` releases it.
    capture_file = nullptr;
}

ssize_t PCAPBlockInputFormat::readFromInput(void * cookie, char * buf, size_t size)
{
    auto & state = *static_cast<InputStreamCookie *>(cookie);
    /// Called from `libpcap` through `stdio`, so an exception must not escape; it is kept and
    /// rethrown once `libpcap` reports the failed read.
    try
    {
        return static_cast<ssize_t>(state.in->read(buf, size));
    }
    catch (...)
    {
        state.exception = std::current_exception();
        errno = EIO;
        return -1;
    }
}

int PCAPBlockInputFormat::closeInput(void *)
{
    /// The input buffer is owned by the caller of the format.
    return 0;
}

void PCAPBlockInputFormat::rethrowInputException()
{
    if (stream_cookie.exception)
        std::rethrow_exception(std::exchange(stream_cookie.exception, nullptr));
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

/// The size of the Ethernet II and IEEE 802.3 headers.
constexpr UInt32 ethernet_header_size = 14;

/// Dissects an IEEE 802.3 frame: the 14-byte header ends with the length of the frame data
/// instead of an EtherType, and the data starts with an LLC header. When both SAPs are 0xAA and
/// the control field is 0x03 (unnumbered information), a SNAP header follows, and the protocol
/// it names (for example IP) is dissected further. `Tins::Dot3` always dissects its data as a
/// plain LLC, so the layers are assembled here.
Tins::PDU * dissectDot3(const uint8_t * data, UInt32 caplen, UInt16 length)
{
    static constexpr UInt32 header_size = ethernet_header_size;
    auto dot3 = std::make_unique<Tins::Dot3>(data, header_size);

    /// The bytes past the length are the padding of a short frame or a trailer, not LLC data.
    const uint8_t * llc_data = data + header_size;
    const UInt32 llc_size = std::min<UInt32>(caplen - header_size, length);

    static constexpr UInt32 snap_header_size = 8;
    if (llc_size >= snap_header_size && llc_data[0] == 0xAA && llc_data[1] == 0xAA && llc_data[2] == 0x03)
        dot3->inner_pdu(new Tins::SNAP(llc_data, llc_size));
    else if (llc_size >= 3) /// The two SAPs and at least one byte of the control field.
        dot3->inner_pdu(new Tins::LLC(llc_data, llc_size));
    else if (llc_size > 0)
        dot3->inner_pdu(new Tins::RawPDU(llc_data, llc_size));

    return dot3.release();
}

/// Dissects a captured frame into a PDU chain according to the link type of the capture.
/// `libtins` dissects Ethernet, Linux cooked capture, BSD loopback, PPI and 802.11 frames, but
/// returns a raw PDU for the raw IP link types, so they are dispatched here: `DLT_RAW` carries
/// either IP version, told apart by the version nibble, while `DLT_IPV4` and `DLT_IPV6` carry
/// only one. `libtins` also takes every `DLT_EN10MB` frame for Ethernet II, so the IEEE 802.3
/// frames, whose type field is a length of at most 1500, are dispatched here as well. The frames
/// of all the other link types are kept as a single raw PDU.
Tins::PDU * dissectFrameLayers(int dlt, const uint8_t * data, UInt32 caplen)
{
    if (dlt == DLT_EN10MB && caplen >= ethernet_header_size)
    {
        const UInt16 type_or_length = static_cast<UInt16>((data[12] << 8) | data[13]);
        if (type_or_length <= 1500)
            return dissectDot3(data, caplen, type_or_length);
    }
    if (dlt == DLT_RAW || dlt == DLT_IPV4 || dlt == DLT_IPV6)
    {
        const int version = caplen == 0 ? 0 : data[0] >> 4;
        if (version == 4 && dlt != DLT_IPV6)
            return new Tins::IP(data, caplen);
        if (version == 6 && dlt != DLT_IPV4)
            return new Tins::IPv6(data, caplen);
        return new Tins::RawPDU(data, caplen);
    }
    return Tins::Internals::pdu_from_dlt_flag(dlt, data, caplen, /*rawpdu_on_no_match=*/ true);
}

/// Appends to `last` the 802.1Q tags that follow a link-layer header ending at `offset` and were
/// captured completely, when `ether_type`, the type field of that header, names a tag. Returns the
/// offset after the last appended tag; `last` is left pointing to it.
UInt32 appendCompleteVlanTags(Tins::PDU *& last, const uint8_t * data, UInt32 offset, UInt32 caplen, UInt16 ether_type)
{
    static constexpr UInt32 vlan_tag_size = 4;
    while ((ether_type == Tins::Constants::Ethernet::VLAN || ether_type == Tins::Constants::Ethernet::QINQ
            || ether_type == Tins::Constants::Ethernet::OLD_QINQ)
        && offset + vlan_tag_size <= caplen)
    {
        auto * tag = new Tins::Dot1Q(data + offset, vlan_tag_size);
        last->inner_pdu(tag);
        last = tag;
        offset += vlan_tag_size;
        ether_type = tag->payload_type();
    }
    return offset;
}

/// Rebuilds the PDU chain of an Ethernet frame from its outer headers that were captured
/// completely - the Ethernet II header and its 802.1Q tags, or the IEEE 802.3 header and its
/// `SNAP` header - and keeps the rest of the frame as a raw PDU. Used when the snapshot length cut
/// the frame inside a header that `libtins` requires to be complete.
Tins::PDU * dissectCompleteEthernetHeaders(const uint8_t * data, UInt32 caplen)
{
    const UInt16 type_or_length = static_cast<UInt16>((data[12] << 8) | data[13]);

    std::unique_ptr<Tins::PDU> root;
    Tins::PDU * last = nullptr;
    UInt32 offset = ethernet_header_size;
    UInt32 end = caplen;
    if (type_or_length <= 1500)
    {
        root = std::make_unique<Tins::Dot3>(data, ethernet_header_size);
        last = root.get();

        /// The bytes past the length are the padding of a short frame or a trailer, not LLC data.
        end = ethernet_header_size + std::min<UInt32>(caplen - ethernet_header_size, type_or_length);
        static constexpr UInt32 snap_header_size = 8;
        const uint8_t * llc_data = data + offset;
        if (end - offset >= snap_header_size && llc_data[0] == 0xAA && llc_data[1] == 0xAA && llc_data[2] == 0x03)
        {
            auto * snap = new Tins::SNAP(llc_data, snap_header_size);
            last->inner_pdu(snap);
            last = snap;
            offset += snap_header_size;
        }
    }
    else
    {
        root = std::make_unique<Tins::EthernetII>(data, ethernet_header_size);
        last = root.get();

        offset = appendCompleteVlanTags(last, data, offset, caplen, type_or_length);
    }

    if (offset < end)
        last->inner_pdu(new Tins::RawPDU(data + offset, end - offset));
    return root.release();
}

Tins::PDU * dissectFrame(int dlt, const uint8_t * data, UInt32 caplen);

/// Rebuilds the PDU chain of a frame that `libtins` rejected with `malformed_packet` from its outer
/// headers that were captured completely, and keeps the rest of the frame as a raw PDU: the
/// Ethernet headers (see `dissectCompleteEthernetHeaders`), the Linux cooked capture header and
/// its 802.1Q tags, the BSD loopback header, or the PPI header followed by the frame it carries,
/// which is dissected according to the link type named in the PPI header. A frame cut inside its
/// outermost header, a raw IP frame and a frame of another link type are kept raw as a whole.
/// `libtins` offers no way to build a radiotap header without the 802.11 frame it carries, so a
/// radiotap frame whose 802.11 header was cut is kept raw as well.
Tins::PDU * dissectCompleteOuterHeaders(int dlt, const uint8_t * data, UInt32 caplen)
{
    if (dlt == DLT_EN10MB && caplen >= ethernet_header_size)
        return dissectCompleteEthernetHeaders(data, caplen);

    static constexpr UInt32 sll_header_size = 16;
    if (dlt == DLT_LINUX_SLL && caplen >= sll_header_size)
    {
        auto sll = std::make_unique<Tins::SLL>(data, sll_header_size);
        Tins::PDU * last = sll.get();
        const UInt32 offset = appendCompleteVlanTags(last, data, sll_header_size, caplen, sll->protocol());
        if (offset < caplen)
            last->inner_pdu(new Tins::RawPDU(data + offset, caplen - offset));
        return sll.release();
    }

    /// The address family is in the byte order of the capturing host, and `Tins::Loopback` keeps it
    /// as it was read. Its constructor from bytes always dissects the carried packet, so the header
    /// is filled in here.
    static constexpr UInt32 loopback_header_size = 4;
    if (dlt == DLT_NULL && caplen >= loopback_header_size)
    {
        auto loopback = std::make_unique<Tins::Loopback>();
        UInt32 family = 0;
        memcpy(&family, data, loopback_header_size);
        loopback->family(family);
        if (caplen > loopback_header_size)
            loopback->inner_pdu(new Tins::RawPDU(data + loopback_header_size, caplen - loopback_header_size));
        return loopback.release();
    }

    /// The PPI header starts with a version, flags and its own length, little-endian, which
    /// includes the fields that follow the fixed part.
    static constexpr UInt32 ppi_fixed_header_size = 8;
    if (dlt == DLT_PPI && caplen >= ppi_fixed_header_size)
    {
        const UInt32 ppi_length = static_cast<UInt32>(data[2] | (data[3] << 8));
        if (ppi_length >= ppi_fixed_header_size && ppi_length <= caplen)
        {
            auto ppi = std::make_unique<Tins::PPI>(data, ppi_length);
            if (ppi_length < caplen)
                ppi->inner_pdu(dissectFrame(static_cast<int>(ppi->dlt()), data + ppi_length, caplen - ppi_length));
            return ppi.release();
        }
    }

    return new Tins::RawPDU(data, caplen);
}

/// Dissects a captured frame like `dissectFrameLayers`, but also when the snapshot length of the
/// capture cut it before the end of a header that `libtins` requires to be complete, such as the
/// `IPv4` or `IPv6` header or a VLAN tag (`IP` itself handles a cut transport header). `libtins`
/// reports such a frame with `malformed_packet`; it is a valid record of the capture, so the frame
/// is dissected up to its last complete outer header and the rest is kept raw (see
/// `dissectCompleteOuterHeaders`). A raw IP frame whose IP header was cut is kept raw as a whole;
/// `outerLayerType` still names its layer.
Tins::PDU * dissectFrame(int dlt, const uint8_t * data, UInt32 caplen)
{
    try
    {
        return dissectFrameLayers(dlt, data, caplen);
    }
    catch (const Tins::malformed_packet &)
    {
        return dissectCompleteOuterHeaders(dlt, data, caplen);
    }
}

/// Type of the outermost layer of a dissected frame. The link type of a raw IP capture names
/// the outermost layer even when the IP header was cut by the snapshot length and the frame was
/// kept raw: `DLT_IPV4` and `DLT_IPV6` carry a single IP version, and `DLT_RAW` tells them apart
/// by the version nibble, which is the first captured byte.
Tins::PDU::PDUType outerLayerType(int dlt, const uint8_t * data, UInt32 caplen, const Tins::PDU & root)
{
    if (root.pdu_type() != Tins::PDU::RAW || caplen == 0)
        return root.pdu_type();

    const int version = data[0] >> 4;
    if ((dlt == DLT_RAW || dlt == DLT_IPV4) && version == 4)
        return Tins::PDU::IP;
    if ((dlt == DLT_RAW || dlt == DLT_IPV6) && version == 6)
        return Tins::PDU::IPv6;
    return root.pdu_type();
}

/// The protocol named by the type field of an Ethernet II header, an 802.1Q tag, a `SNAP` header
/// or a Linux cooked capture header, or by the address family of a BSD loopback header, or
/// `UNKNOWN` for other layers and for type values that `libtins` does not know.
Tins::PDU::PDUType carriedProtocol(const Tins::PDU & layer)
{
    UInt16 type = 0;
    if (const auto * eth = dynamic_cast<const Tins::EthernetII *>(&layer))
        type = eth->payload_type();
    else if (const auto * tag = dynamic_cast<const Tins::Dot1Q *>(&layer))
        type = tag->payload_type();
    else if (const auto * snap = dynamic_cast<const Tins::SNAP *>(&layer))
        type = snap->eth_type();
    else if (const auto * sll = dynamic_cast<const Tins::SLL *>(&layer))
        type = sll->protocol();
    else if (const auto * loopback = dynamic_cast<const Tins::Loopback *>(&layer))
    {
        /// The address family is in the byte order of the capturing host. `AF_INET` is 2 everywhere,
        /// while `AF_INET6` is 24 on NetBSD and OpenBSD, 28 on FreeBSD and 30 on macOS.
        UInt32 family = loopback->family();
        if (family > 0xFFFF)
            family = std::byteswap(family);
        if (family == 2)
            return Tins::PDU::IP;
        if (family == 24 || family == 28 || family == 30)
            return Tins::PDU::IPv6;
        return Tins::PDU::UNKNOWN;
    }
    else
        return Tins::PDU::UNKNOWN;
    return Tins::Internals::ether_type_to_pdu_flag(static_cast<Tins::Constants::Ethernet::e>(type));
}

/// The captured part of an outermost IP header that the snapshot length cut, so that `libtins`
/// did not dissect it.
struct CutIPHeader
{
    UInt8 version = 0;
    const uint8_t * data = nullptr;
    size_t size = 0;
};

/// Finds the outermost IP header cut by the snapshot length in a frame without a dissected IP
/// layer: the raw tail of the frame (see `dissectCompleteOuterHeaders`) is such a header when the
/// layer before it, or the link type of a raw IP capture, names IP, and its version nibble agrees.
std::optional<CutIPHeader> findCutIPHeader(int dlt, const uint8_t * data, UInt32 caplen, const Tins::PDU & root)
{
    const Tins::PDU * carrier = nullptr;
    const Tins::PDU * last = &root;
    while (last->inner_pdu() != nullptr)
    {
        carrier = last;
        last = last->inner_pdu();
    }

    const auto * raw = dynamic_cast<const Tins::RawPDU *>(last);
    if (raw == nullptr || raw->payload().empty())
        return {};

    const auto expected = carrier == nullptr ? outerLayerType(dlt, data, caplen, root) : carriedProtocol(*carrier);
    const UInt8 version = raw->payload()[0] >> 4;
    if ((expected == Tins::PDU::IP && version == 4) || (expected == Tins::PDU::IPv6 && version == 6))
        return CutIPHeader{version, raw->payload().data(), raw->payload().size()};
    return {};
}

/// The IPv6 extension headers (RFC 8200 and RFC 7045). `No Next Header` (59) and `ESP` (50)
/// end the chain, so they are not listed here.
bool isIPv6ExtensionHeader(UInt8 header)
{
    switch (header)
    {
        case Tins::IPv6::HOP_BY_HOP:
        case Tins::IPv6::ROUTING:
        case Tins::IPv6::FRAGMENT:
        case Tins::IPv6::AUTHENTICATION:
        case Tins::IPv6::DESTINATION_OPTIONS:
        case Tins::IPv6::MOBILITY:
        case 139: /// Host Identity Protocol
        case 140: /// Shim6
            return true;
        default:
            return false;
    }
}

struct IPv6HeaderChain
{
    /// The protocol carried by the packet.
    UInt8 next_header = 0;
    /// Byte offset in the frame where the extension-header chain ends.
    size_t end = 0;
};

/// Walks the extension-header chain (Hop-by-Hop options, Routing, Fragment, ...) of an IPv6
/// packet, calling `on_extension_header` with the type of every extension header in the chain,
/// and returns the protocol carried by the packet: the `next header` value of the last header of
/// the chain, or the value from the fixed header when there is no chain, together with the offset
/// where the chain ends. `libtins` strips the `next header` byte of every extension header it
/// parses and exposes only the one from the fixed header, so the chain is walked over the raw
/// bytes of the frame. When the snapshot length cuts the chain, the type of the last extension
/// header that is still known is returned: the walk stops at the first extension header that was
/// not captured completely. The walk also stops after the Fragment header of a non-first fragment:
/// the headers that follow it are present only in the first fragment (RFC 8200, section 4.5), so
/// the bytes after it are fragment data, and only the type it names is known.
template <typename OnExtensionHeader>
IPv6HeaderChain walkIPv6ExtensionHeaders(
    const Tins::IPv6 & ipv6, const unsigned char * frame, size_t frame_size, size_t ipv6_offset, OnExtensionHeader && on_extension_header)
{
    static constexpr size_t fixed_header_size = 40;

    UInt8 next = ipv6.next_header();
    size_t pos = ipv6_offset + fixed_header_size;
    while (isIPv6ExtensionHeader(next))
    {
        on_extension_header(next);

        /// Every extension header starts with its own `next header` byte followed by a length byte.
        if (pos + 2 > frame_size)
            break;

        const UInt8 following = frame[pos];
        const UInt8 length_field = frame[pos + 1];
        size_t header_length = 0;
        if (next == Tins::IPv6::FRAGMENT)
            header_length = 8; /// Fixed size; the second byte is reserved.
        else if (next == Tins::IPv6::AUTHENTICATION)
            header_length = (static_cast<size_t>(length_field) + 2) * 4; /// In 4-byte units, minus 2.
        else
            header_length = (static_cast<size_t>(length_field) + 1) * 8; /// In 8-byte units, not including the first 8.

        /// A header cut by the snapshot length is the last one that is known: what it points to
        /// was not captured, and taking its `next header` byte would report a transport protocol
        /// whose bytes are not there.
        if (pos + header_length > frame_size)
            break;

        /// The fragment offset is in the upper 13 bits of the third and the fourth bytes.
        const bool is_non_first_fragment = next == Tins::IPv6::FRAGMENT && ((frame[pos + 2] << 8) | frame[pos + 3]) >> 3 != 0;

        pos += header_length;
        next = following;

        if (is_non_first_fragment)
            break;
    }
    return {next, pos};
}

UInt8 ipv6FinalNextHeader(const Tins::IPv6 & ipv6, const unsigned char * frame, size_t frame_size, size_t ipv6_offset)
{
    return walkIPv6ExtensionHeaders(ipv6, frame, frame_size, ipv6_offset, [](UInt8) {}).next_header;
}

/// Size of the header of `layer`, which starts at `offset` in the frame, as it was captured.
/// `PDU::header_size` is computed again from the fields that `libtins` kept, so it differs from
/// the captured size when the header has bytes that are not kept: the padding after the
/// end-of-options marker of `IPv4` and `TCP`, or the padding of `IPv6` extension headers.
size_t capturedHeaderSize(const Tins::PDU & layer, const unsigned char * frame, size_t frame_size, size_t offset)
{
    if (const auto * ip = dynamic_cast<const Tins::IP *>(&layer))
        return ip->head_len() * sizeof(uint32_t);
    if (const auto * tcp = dynamic_cast<const Tins::TCP *>(&layer))
        return tcp->data_offset() * sizeof(uint32_t);
    if (const auto * ipv6 = dynamic_cast<const Tins::IPv6 *>(&layer))
        return walkIPv6ExtensionHeaders(*ipv6, frame, frame_size, offset, [](UInt8) {}).end - offset;
    return layer.header_size();
}

/// Byte offset of `target` inside the frame that starts with `root`: the sum of the captured
/// header sizes of the layers above it.
size_t offsetOfLayer(const Tins::PDU & root, const Tins::PDU & target, const unsigned char * frame, size_t frame_size)
{
    size_t offset = 0;
    for (const Tins::PDU * layer = &root; layer != nullptr && layer != &target; layer = layer->inner_pdu())
        offset += capturedHeaderSize(*layer, frame, frame_size, offset);
    return offset;
}

/// Name of the IANA protocol number found in the IPv4 `protocol` field or after the IPv6
/// extension-header chain. Numbers without a well-known name are rendered in decimal.
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
        case 135: return "Mobility Header";
        case 136: return "UDPLite";
        case 137: return "MPLS-in-IP";
        case 139: return "HIP";
        case 140: return "Shim6";
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
    std::array<bool, COL_COUNT> need{};
    for (const std::string & name : header.getNames())
        need[getColumnIdx(name)] = true;

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

    /// A row can carry the whole packet in `raw` (and most of it again in `payload`), so the
    /// block is bounded by the packet bytes it stores as well as by the number of rows. The byte
    /// budget counts the captured bytes of every packet when `raw` is materialized, plus the
    /// payload bytes copied into `payload` when it is materialized. Reads of metadata columns only
    /// are bounded by rows alone. The row budget of the query (`max_block_size`) caps the
    /// `PCAP`-specific one.
    size_t max_rows = format_settings.pcap.max_block_size;
    if (max_block_size_rows != 0)
        max_rows = std::min(max_rows, max_block_size_rows);
    const size_t max_bytes = format_settings.pcap.prefer_block_bytes;

    size_t num_rows = 0;
    size_t bytes_read = 0;
    size_t block_bytes = 0;

    while (num_rows < max_rows && (max_bytes == 0 || block_bytes < max_bytes))
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
        {
            rethrowInputException();
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Failed to read packet from PCAP: {}", pcap_geterr(handle));
        }

        const UInt32 caplen = pkthdr->caplen;
        const UInt32 wire_len = pkthdr->len;

        /// Dissect the raw bytes into a PDU chain; undissected tails are kept as a RAW pdu.
        std::unique_ptr<Tins::PDU> pdu_owner(dissectFrame(dlt, data, caplen));
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
            String name = Tins::Utils::to_string(outerLayerType(dlt, data, caplen, *pdu));
            col_link_type->insertData(name.data(), name.size());
        }

        if (need[COL_PROTOCOLS])
        {
            for (const Tins::PDU * p = pdu; p != nullptr; p = p->inner_pdu())
            {
                String name = Tins::Utils::to_string(p->pdu_type());
                col_protocols_data->insertData(name.data(), name.size());

                /// `libtins` does not keep the IPv6 extension headers in the PDU chain, so they
                /// are taken from the raw bytes and listed right after the IPv6 layer.
                if (const auto * ipv6_layer = dynamic_cast<const Tins::IPv6 *>(p))
                {
                    walkIPv6ExtensionHeaders(*ipv6_layer, data, caplen, offsetOfLayer(*pdu, *ipv6_layer, data, caplen), [&](UInt8 extension_header)
                    {
                        String header_name = ipProtocolName(extension_header);
                        col_protocols_data->insertData(header_name.data(), header_name.size());
                    });
                }
            }
            col_protocols_offsets->insertValue(col_protocols_data->size());
        }

        /// Ethernet. The columns describe the outermost link layer only: a recursive `find_pdu`
        /// would also find a tunneled Ethernet frame and report its header and VLAN tag as if
        /// they belonged to the captured packet.
        auto insert_null_string = [](ColumnString & col, ColumnUInt8 & nullmap) { col.insertDefault(); nullmap.insertValue(1); };
        const auto * eth = dynamic_cast<const Tins::EthernetII *>(pdu);
        const auto * dot3 = dynamic_cast<const Tins::Dot3 *>(pdu);
        if (eth || dot3)
        {
            if (need[COL_ETH_SRC]) { String s = eth ? eth->src_addr().to_string() : dot3->src_addr().to_string(); col_eth_src->insertData(s.data(), s.size()); col_eth_src_null->insertValue(0); }
            if (need[COL_ETH_DST]) { String s = eth ? eth->dst_addr().to_string() : dot3->dst_addr().to_string(); col_eth_dst->insertData(s.data(), s.size()); col_eth_dst_null->insertValue(0); }
        }
        else
        {
            if (need[COL_ETH_SRC]) insert_null_string(*col_eth_src, *col_eth_src_null);
            if (need[COL_ETH_DST]) insert_null_string(*col_eth_dst, *col_eth_dst_null);
        }

        if (need[COL_VLAN_ID])
        {
            /// The outer 802.1Q tag directly follows the link-layer header.
            if (const auto * vlan = dynamic_cast<const Tins::Dot1Q *>(pdu->inner_pdu()))
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
        /// `DOT1Q`; the tag itself is exposed separately as `vlan_id`. The SNAP header of an
        /// IEEE 802.3 frame is unwrapped the same way, so it reports the protocol named in it.
        if (need[COL_ETH_TYPE])
        {
            String name;
            if (eth || dot3)
            {
                /// The header whose type field names `inner`.
                const Tins::PDU * carrier = pdu;
                const Tins::PDU * inner = pdu->inner_pdu();
                while (inner != nullptr && inner->pdu_type() == Tins::PDU::DOT1Q)
                {
                    carrier = inner;
                    inner = inner->inner_pdu();
                }
                /// The SNAP header is unwrapped also when the snapshot length left nothing after
                /// it, so that its type field names the carried protocol.
                if (inner != nullptr && inner->pdu_type() == Tins::PDU::SNAP)
                {
                    carrier = inner;
                    inner = inner->inner_pdu();
                }

                std::optional<Tins::PDU::PDUType> type;
                if (inner != nullptr)
                    type = inner->pdu_type();

                /// When the snapshot length cut the header of the carried protocol, or left none of
                /// it, the type field of the carrier still names it. A cut 802.1Q tag is not named:
                /// the protocol encapsulated by it is not known.
                if (!type || *type == Tins::PDU::RAW)
                {
                    const auto carried = carriedProtocol(*carrier);
                    if (carried != Tins::PDU::UNKNOWN && carried != Tins::PDU::DOT1Q && carried != Tins::PDU::DOT1AD)
                        type = carried;
                }

                if (type)
                    name = Tins::Utils::to_string(*type);
            }
            col_eth_type->insertData(name.data(), name.size());
        }

        /// IP layer (v4 or v6): the first one in the chain. Searching for each version separately
        /// would also find the inner packet of a tunnel (`IPv4` in `IPv6` or the reverse), and the
        /// columns would then mix the outer and the inner headers.
        const Tins::IP * ipv4 = nullptr;
        const Tins::IPv6 * ipv6 = nullptr;
        for (const Tins::PDU * p = pdu; p != nullptr && ipv4 == nullptr && ipv6 == nullptr; p = p->inner_pdu())
        {
            ipv4 = dynamic_cast<const Tins::IP *>(p);
            ipv6 = dynamic_cast<const Tins::IPv6 *>(p);
        }

        /// When the snapshot length cut the outer IP header, the fields that were captured are
        /// still taken from its bytes, so that the packet is not mistaken for a non-IP one.
        std::optional<CutIPHeader> cut_ip;
        if (ipv4 == nullptr && ipv6 == nullptr)
            cut_ip = findCutIPHeader(dlt, data, caplen, *pdu);

        /// The byte at `offset` of the cut IP header, if it was captured.
        auto cut_ip_byte = [&](size_t offset) -> std::optional<UInt8>
        {
            if (cut_ip && offset < cut_ip->size)
                return cut_ip->data[offset];
            return {};
        };

        if (need[COL_IP_VERSION])
        {
            if (ipv4) { col_ip_version->insertValue(4); col_ip_version_null->insertValue(0); }
            else if (ipv6) { col_ip_version->insertValue(6); col_ip_version_null->insertValue(0); }
            else if (cut_ip) { col_ip_version->insertValue(cut_ip->version); col_ip_version_null->insertValue(0); }
            else { col_ip_version->insertDefault(); col_ip_version_null->insertValue(1); }
        }

        if (need[COL_SRC_ADDR] || need[COL_DST_ADDR])
        {
            IPv6 src{};
            IPv6 dst{};
            bool has_src = true;
            bool has_dst = true;
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
            else if (cut_ip)
            {
                /// Each address is filled only when all of its bytes were captured.
                const size_t address_size = cut_ip->version == 4 ? 4 : 16;
                const size_t src_offset = cut_ip->version == 4 ? 12 : 8;
                const size_t dst_offset = src_offset + address_size;
                auto write_address = [&](size_t offset, IPv6 & out)
                {
                    if (offset + address_size > cut_ip->size)
                        return false;
                    if (address_size == 4)
                    {
                        UInt32 address_be = 0;
                        memcpy(&address_be, cut_ip->data + offset, 4);
                        writeIPv4Mapped(address_be, out);
                    }
                    else
                        memcpy(&out.toUnderType(), cut_ip->data + offset, 16);
                    return true;
                };
                has_src = write_address(src_offset, src);
                has_dst = write_address(dst_offset, dst);
            }
            else
            {
                has_src = false;
                has_dst = false;
            }

            if (need[COL_SRC_ADDR]) { col_src_addr->insertValue(src); col_src_addr_null->insertValue(has_src ? 0 : 1); }
            if (need[COL_DST_ADDR]) { col_dst_addr->insertValue(dst); col_dst_addr_null->insertValue(has_dst ? 0 : 1); }
        }

        if (need[COL_IP_TTL])
        {
            std::optional<UInt8> cut_ttl;
            if (cut_ip)
                cut_ttl = cut_ip_byte(cut_ip->version == 4 ? 8 : 7);

            if (ipv4) { col_ip_ttl->insertValue(ipv4->ttl()); col_ip_ttl_null->insertValue(0); }
            else if (ipv6) { col_ip_ttl->insertValue(ipv6->hop_limit()); col_ip_ttl_null->insertValue(0); }
            else if (cut_ttl) { col_ip_ttl->insertValue(*cut_ttl); col_ip_ttl_null->insertValue(0); }
            else { col_ip_ttl->insertDefault(); col_ip_ttl_null->insertValue(1); }
        }

        /// Transport fields describe the same outer IP layer as the address fields.
        Tins::PDU * ip_inner = ipv4 ? ipv4->inner_pdu() : (ipv6 ? ipv6->inner_pdu() : nullptr);
        auto * tcp = dynamic_cast<Tins::TCP *>(ip_inner);
        auto * udp = dynamic_cast<Tins::UDP *>(ip_inner);

        /// Taken from the IP header itself (the IPv4 `protocol` field, or the `next header`
        /// field after the IPv6 extension-header chain), so it stays correct when the transport
        /// header was not captured (a truncated snapshot) or is not decodable (a non-first fragment).
        if (need[COL_IP_PROTOCOL])
        {
            String name;
            if (ipv4)
                name = ipProtocolName(ipv4->protocol());
            else if (ipv6)
                name = ipProtocolName(ipv6FinalNextHeader(*ipv6, data, caplen, offsetOfLayer(*pdu, *ipv6, data, caplen)));
            else if (cut_ip)
            {
                /// The extension headers of a cut IPv6 header were not captured, so the type of the
                /// first one is reported, as for an extension header cut by the snapshot length.
                if (const auto protocol = cut_ip_byte(cut_ip->version == 4 ? 9 : 6))
                    name = ipProtocolName(*protocol);
            }
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
            const Tins::PDU * inner = nullptr;
            if (tcp) inner = tcp->inner_pdu();
            else if (udp) inner = udp->inner_pdu();

            /// `libtins` keeps the bytes after the TCP or UDP header as a raw PDU, which copies them
            /// from the captured frame. They leave out the bytes past the IP length, such as the
            /// Ethernet padding of short frames.
            if (const auto * raw_payload = dynamic_cast<const Tins::RawPDU *>(inner))
            {
                const auto & bytes = raw_payload->payload();
                payload = std::string_view(reinterpret_cast<const char *>(bytes.data()), bytes.size());
            }

            if (need[COL_PAYLOAD_LENGTH])
                col_payload_length->insertValue(static_cast<UInt32>(payload.size()));
            if (need[COL_PAYLOAD])
            {
                col_payload->insertData(payload.data(), payload.size());
                block_bytes += payload.size();
            }
        }

        if (need[COL_RAW])
        {
            col_raw->insertData(reinterpret_cast<const char *>(data), caplen);
            block_bytes += caplen;
        }
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

    /// The requested header may declare types other than the ones produced above.
    const auto & native_types = getColumnTypes();
    Columns cols;
    cols.reserve(header.columns());
    for (const auto & requested : header)
    {
        size_t idx = getColumnIdx(requested.name);
        ColumnWithTypeAndName column(built[idx], native_types[idx], requested.name);
        castColumnToRequestedType(column, requested.type);
        cols.push_back(std::move(column.column));
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
    factory.registerInputFormat(
        "PCAP",
        [](ReadBuffer & buf,
           const Block & sample,
           const RowInputFormatParams & params,
           const FormatSettings & settings) -> InputFormatPtr
        {
            return std::make_shared<PCAPBlockInputFormat>(
                buf, std::make_shared<const Block>(sample), settings, params.max_block_size_rows);
        });
    factory.markFormatSupportsSubsetOfColumns("PCAP");
    /// `registerInputFormat` maps only the `.pcap` extension, derived from the format name.
    factory.registerFileExtension("pcapng", "PCAP");

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
queried with SQL. It is available on Linux builds.

Parsing is performed with the `libtins` library, which reads the capture
container through `libpcap`. Only reading of capture files is supported; live
capture is not.

:::note
`pcapng` support is limited to the subset that `libpcap` accepts: every
interface description block in the file must declare the same link type and
the same snapshot length. A capture that mixes interfaces with different link
types or snapshot lengths - for example one produced by `dumpcap -i eth0 -i
lo` - is rejected with an error, either when the file is opened or when the
differing interface description block is reached while reading.
:::

:::note
Frames are dissected according to the link type of the capture. The supported
link types are Ethernet (`LINKTYPE_ETHERNET`), Linux cooked capture
(`LINKTYPE_LINUX_SLL`), BSD loopback (`LINKTYPE_NULL`), PPI (`LINKTYPE_PPI`),
IEEE 802.11 with or without a radiotap header (`LINKTYPE_IEEE802_11`,
`LINKTYPE_IEEE802_11_RADIOTAP`), and raw IP (`LINKTYPE_RAW`, `LINKTYPE_IPV4`,
`LINKTYPE_IPV6`). For raw IP captures the outermost layer is the IP header, so
`link_type` is `IP` or `IPv6` and the Ethernet columns are empty. Ethernet
captures may hold both Ethernet II frames (`link_type` is `ETHERNET_II`) and
IEEE 802.3 frames, whose type field is a length of at most 1500 (`link_type` is
`IEEE802_3`). An IEEE 802.3 frame carries an `LLC` header, followed by a `SNAP`
header when both SAPs are `0xAA`; the protocol named in the `SNAP` header, for
example IP, is dissected further. Frames of any other link type are not dissected: they still produce rows with the capture
metadata and `raw`, but `link_type` is `RAW`, `protocols` is `['RAW']`, and the
layer-specific columns are empty.
:::

:::note
When the snapshot length cuts a frame inside a header that has to be complete
to be decoded, such as an IP header or an 802.1Q tag, an Ethernet, Linux
cooked capture, BSD loopback or PPI frame is still dissected up to its last
complete outer header, and the rest is kept raw. IEEE 802.11 frames, with or
without a radiotap header, are not: such a cut 802.11 frame produces a row like
a frame of an unsupported link type, where `link_type` is `RAW`, `protocols` is
`['RAW']`, and the layer-specific columns are empty.
:::

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
| `link_type` | `LowCardinality(String)` | Type of the outermost layer, for example `ETHERNET_II`, `IEEE802_3`, or `IP` / `IPv6` for raw IP captures, also when the IP header was cut by the snapshot length |
| `protocols` | `Array(LowCardinality(String))` | Ordered list of the protocol headers dissected from the captured bytes, from the link layer inwards, for example `['ETHERNET_II', 'IP', 'TCP']`. IPv6 extension headers are listed right after `IPv6` by their IANA keywords: `HOPOPT`, `IPv6-Route`, `IPv6-Frag`, `AH`, `IPv6-Opts`, `Mobility Header`, `HIP` and `Shim6`. The list stops at the last header that could be decoded: when the transport header was cut off by the snapshot length, even partially, or is not decodable, it ends with `IP` / `IPv6` (or `RAW`) even though `ip_protocol` names the transport. Use `ip_protocol` rather than `has(protocols, 'TCP')` to select packets by transport protocol |
| `eth_src` | `Nullable(String)` | Source MAC address of the outermost Ethernet header, or `NULL` when the link layer is not Ethernet |
| `eth_dst` | `Nullable(String)` | Destination MAC address of the outermost Ethernet header |
| `eth_type` | `LowCardinality(String)` | Protocol carried by the outermost Ethernet frame, for example `IP`; empty when the link layer is not Ethernet. 802.1Q tags are unwrapped, so a VLAN-tagged frame reports the encapsulated protocol and not `DOT1Q`; the tag itself is exposed as `vlan_id`. For an IEEE 802.3 frame it is `LLC`, or the protocol named in the `SNAP` header. When the snapshot length cuts the header of that protocol, or leaves none of it, it is still named from the type field, for example `IP`, while `protocols` ends with `RAW` or with the last complete header |
| `vlan_id` | `Nullable(UInt16)` | 802.1Q VLAN identifier of the tag right after the outermost Ethernet header, if present |
| `ip_version` | `Nullable(UInt8)` | Version of the outermost IP header, `4` or `6`, or `NULL` for non-IP packets. When the snapshot length cuts the IP header, it and the other IP columns are still taken from the captured bytes of the header, and the fields that were not captured are `NULL` (or empty for `ip_protocol`) |
| `src_addr` | `Nullable(IPv6)` | Source address of the outermost IP header; IPv4 addresses are mapped to IPv6 (`::ffff:a.b.c.d`). `NULL` when the snapshot length cut it |
| `dst_addr` | `Nullable(IPv6)` | Destination address of the outermost IP header |
| `ip_protocol` | `LowCardinality(String)` | Protocol carried by the outermost IP packet, taken from the IPv4 `protocol` field or from the IPv6 `next header` field after any extension headers (Hop-by-Hop options, Routing, Fragment and so on), for example `TCP`, `UDP` or `ICMP`. When the snapshot length cuts an IPv6 extension header, or the fixed IPv6 header after its `next header` field, it is the type of that header, for example `IPv6-Route`. It is filled even when the transport header itself was not captured or is not in the packet (a fragment other than the first one), in which case `src_port`, `dst_port` and the TCP columns stay `NULL`. Numbers without a well-known name are rendered in decimal. Empty for non-IP packets |
| `ip_ttl` | `Nullable(UInt16)` | IPv4 TTL or IPv6 hop limit of the outermost IP header |
| `src_port` | `Nullable(UInt16)` | Source port of the TCP or UDP header directly inside the outermost IP header |
| `dst_port` | `Nullable(UInt16)` | Destination port of the TCP or UDP header directly inside the outermost IP header |
| `tcp_flags` | `Nullable(String)` | Comma-separated flags of that TCP header, for example `ACK,PSH` |
| `tcp_seq` | `Nullable(UInt32)` | TCP sequence number |
| `tcp_ack` | `Nullable(UInt32)` | TCP acknowledgement number |
| `payload_length` | `UInt32` | Number of bytes in `payload` |
| `payload` | `String` | Bytes carried by that TCP or UDP header. Empty for other transport protocols, for example `ICMP`, and for non-IP packets |
| `raw` | `String` | The full captured frame, starting at the link layer |

Layer-specific columns are `Nullable`, except `eth_type` and `ip_protocol`.
Those two low-cardinality string columns use an empty string when their layer
is absent. Packets that do not contain a given layer (for example a non-IP
packet, or a UDP packet that has no TCP fields) still produce a row.

The TCP or UDP payload is a contiguous range of the raw frame. It only
covers the bytes within the IP length, so bytes after the IP packet, such as the
padding of short Ethernet frames or a link-layer trailer, are part of `raw` but
not of `payload`. When a frame has no such trailing bytes, the payload is a
suffix of `raw`, and
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

You usually do not need to store both `raw` and `payload`. For frames without
bytes after the IP packet, the payload is a suffix of the raw frame, so it can
be recovered from `raw` together with `capture_length` and `payload_length`:

```sql title="Recover the payload from raw"
SELECT substring(raw, (capture_length - payload_length) + 1) AS payload
FROM packets;
```

This does not hold for padded short Ethernet frames (for example a UDP packet
with a few bytes of payload padded to 60 bytes) or frames with a link-layer
trailer: there `raw` ends with the padding, and the query above returns the
padding instead of the payload. Store `payload` as well if your capture can
contain such frames.

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

| Setting | Description | Default |
|---------|-------------|---------|
| `input_format_pcap_max_block_size` | The maximum number of packets (rows) in one block. | `65409` |
| `input_format_pcap_prefer_block_bytes` | A block is finished once the packet bytes it stores reach this amount, even if it has fewer rows than `input_format_pcap_max_block_size`. The count includes the captured bytes of every packet when the `raw` column is read and the payload bytes copied into the `payload` column when it is read, so it bounds the string data of one block for captures with large packets. Reads of other columns only are not limited by bytes. `0` disables the byte limit. | `16744704` |
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
