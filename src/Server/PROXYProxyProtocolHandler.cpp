#include <Server/PROXYProxyProtocolHandler.h>
#include <Common/Exception.h>

#include "Poco/Net/StreamSocket.h"

#include <boost/algorithm/string.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/lexical_cast.hpp>

#include <algorithm>
#include <chrono>
#include <initializer_list>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int PROXY_PROTOCOL_ERROR;
}

namespace
{

struct PROXYHeader
{
    boost::asio::ip::address source_address;
    std::uint16_t source_port = 0;

    boost::asio::ip::address destination_address;
    std::uint16_t destination_port = 0;
};

void peek(
    Poco::Net::StreamSocket & socket,
    std::vector<char> & buffer,
    const std::size_t min_size,
    const std::size_t max_size = 1024
)
{
    // Assuming the buffer contains the correct data from previous calls, so we reuse it.
    if (min_size <= buffer.size())
        return;

    buffer.resize(std::max(min_size, max_size));
    std::size_t read = 0;

    const std::chrono::seconds total_timeout{30};
    const auto start_time = std::chrono::steady_clock::now();

    while (read < min_size)
    {
        // Try and peek the same chunk again and again.
        read = socket.receiveBytes(buffer.data(), buffer.size(), MSG_PEEK);

        if (read <= 0)
        {
            buffer.clear();
            throw Exception("Unexpected end of stream while peeking into the PROXY v1/v2 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);
        }

        if (read < min_size && std::chrono::steady_clock::now() - start_time >= total_timeout)
            throw Exception("Timeout reached while trying to peek into data big enough to parse PROXY v1/v2 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);
    }

    buffer.resize(read);
}

void read(
    Poco::Net::StreamSocket & socket,
    std::vector<char> & buffer,
    const std::size_t size
)
{
    buffer.clear();
    buffer.resize(size);
    std::size_t offset = 0;

    while (offset < buffer.size())
    {
        const auto read = socket.receiveBytes(buffer.data() + offset, buffer.size() - offset);

        if (read <= 0)
        {
            buffer.resize(offset);
            throw Exception("Unexpected end of stream while consuming the PROXY v1/v2 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);
        }

        offset += read;
    }
}

bool match(
    Poco::Net::StreamSocket & socket,
    std::vector<char> & buffer,
    const std::size_t start,
    const std::initializer_list<char> & chunk
)
{
    peek(socket, buffer, start + chunk.size());
    return std::equal(chunk.begin(), chunk.end(), buffer.begin() + start);
}

std::size_t peekUntil(
    Poco::Net::StreamSocket & socket,
    std::vector<char> & buffer,
    const std::initializer_list<char> & mark,
    const std::size_t start = 0,
    const std::size_t end = 1024
)
{
    // Try to pre-peek generously upfront.
    peek(socket, buffer, start + mark.size(), end + mark.size());

    for (std::size_t i = start; i < end; ++i)
    {
        if (match(socket, buffer, i, mark))
            return i;
    }

    throw Exception("Unable to locate the mark in the first " + std::to_string(end) + " bytes of the stream while peeking into the PROXY v1 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);
}

PROXYHeader tryReadPROXYv1Header(Poco::Net::StreamSocket & socket, std::vector<char> & buffer)
{
    PROXYHeader header;
    std::size_t offset = 6;

    if (
        match(socket, buffer, offset, {'T', 'C', 'P', '4', ' '}) ||
        match(socket, buffer, offset, {'T', 'C', 'P', '6', ' '})
    )
    {
        offset += 6;

        auto tok_end = peekUntil(socket, buffer, {' '}, offset);
        header.source_address = boost::asio::ip::make_address(std::string_view(&buffer[offset], tok_end - offset));
        offset = tok_end + 1;

        tok_end = peekUntil(socket, buffer, {' '}, offset);
        header.destination_address = boost::asio::ip::make_address(std::string_view(&buffer[offset], tok_end - offset));
        offset = tok_end + 1;

        tok_end = peekUntil(socket, buffer, {' '}, offset);
        header.source_port = boost::lexical_cast<std::uint16_t>(std::string_view(&buffer[offset], tok_end - offset));
        offset = tok_end + 1;

        tok_end = peekUntil(socket, buffer, {'\r', '\n'}, offset);
        header.destination_port = boost::lexical_cast<std::uint16_t>(std::string_view(&buffer[offset], tok_end - offset));
        offset = tok_end + 2;

        read(socket, buffer, offset);
        return header;
    }
    else if (
        match(socket, buffer, offset, {'U', 'N', 'K', 'N', 'O', 'W', 'N', '\r'}) ||
        match(socket, buffer, offset, {'U', 'N', 'K', 'N', 'O', 'W', 'N', ' '})
    )
    {
        offset += 7;

        const auto line_end = peekUntil(socket, buffer, {'\r', '\n'}, offset);
        offset += line_end + 2;

        read(socket, buffer, offset);
        return header;
    }

    throw Exception("Unable to parse PROXY v1 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);
}

PROXYHeader tryReadPROXYv2Header(Poco::Net::StreamSocket & socket, std::vector<char> & buffer)
{
    PROXYHeader header;
    std::size_t offset = 12;

    // Peek the metadata of the header first.
    peek(socket, buffer, 16);

    const std::uint8_t ver_cmd = buffer[offset];
    const std::uint8_t fam_addr = buffer[offset + 1];
    const std::uint16_t addr_len = ntohs(*reinterpret_cast<std::uint16_t *>(&buffer[offset + 2]));

    offset = 16; // start of the address

    switch (ver_cmd)
    {
        case '\x20': // v2 LOCAL
        {
            if (fam_addr != '\x00') // UNSPEC
                throw Exception("Unsupported forwarded connection type (in LOCAL command) in PROXY v2 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);

            if (addr_len != 0)
                throw Exception("Bad address length (in LOCAL command) in PROXY v2 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);

            header.source_address = boost::asio::ip::make_address(socket.peerAddress().host().toString());
            header.source_port = socket.peerAddress().port();
            header.destination_address = boost::asio::ip::make_address(socket.address().host().toString());
            header.destination_port = socket.address().port();

            read(socket, buffer, 16 + addr_len);
            return header;
        }

        case '\x21': // v2 PROXY
        {
            switch (fam_addr)
            {
                case '\x11': // TCP over IPv4
                {
                    // Peek the entire header as we know its length here.
                    peek(socket, buffer, offset + addr_len);

                    struct Addr
                    {
                        std::uint32_t src_addr;
                        std::uint32_t dst_addr;
                        std::uint16_t src_port;
                        std::uint16_t dst_port;
                    };

                    Addr & addr = *reinterpret_cast<Addr *>(&buffer[offset]);

                    header.source_address = boost::asio::ip::make_address_v4(reinterpret_cast<char *>(&addr.src_addr));
                    header.source_port = ntohs(addr.src_port);
                    header.destination_address = boost::asio::ip::make_address_v4(reinterpret_cast<char *>(&addr.dst_addr));
                    header.destination_port = ntohs(addr.dst_port);

                    read(socket, buffer, offset + addr_len);
                    return header;
                }

                case '\x21': // TCP over IPv6
                {
                    // Peek the entire header as we know its length here.
                    peek(socket, buffer, offset + addr_len);

                    struct Addr
                    {
                        std::uint8_t src_addr[16];
                        std::uint8_t dst_addr[16];
                        std::uint16_t src_port;
                        std::uint16_t dst_port;
                    };

                    Addr & addr = *reinterpret_cast<Addr *>(&buffer[offset]);

                    header.source_address = boost::asio::ip::make_address_v6(reinterpret_cast<char *>(addr.src_addr));
                    header.source_port = ntohs(addr.src_port);
                    header.destination_address = boost::asio::ip::make_address_v6(reinterpret_cast<char *>(addr.src_addr));
                    header.destination_port = ntohs(addr.dst_port);

                    read(socket, buffer, offset + addr_len);
                    return header;
                }
            }

            throw Exception("Unsupported forwarded connection type in PROXY v2 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);
        }
    }

    throw Exception("Unsupported version/command in PROXY v2 protocol header", ErrorCodes::PROXY_PROTOCOL_ERROR);
}

std::optional<PROXYHeader> tryReadPROXYHeader(Poco::Net::StreamSocket & socket, bool expect_v1, bool expect_v2)
{
    std::vector<char> buffer;

    if (expect_v1 && match(socket, buffer, 0, {'P', 'R', 'O', 'X', 'Y', ' '}))
        return tryReadPROXYv1Header(socket, buffer);

    if (expect_v2 && match(socket, buffer, 0, {'\x0D', '\x0A', '\x0D', '\x0A', '\x00', '\x0D', '\x0A', '\x51', '\x55', '\x49', '\x54', '\x0A'}))
        return tryReadPROXYv2Header(socket, buffer);

    return {};
}

}

PROXYProxyProtocolHandler::PROXYProxyProtocolHandler(const PROXYProxyConfig & config_)
    : config(config_)
{
}

void PROXYProxyProtocolHandler::handle(Poco::Net::StreamSocket & socket)
{
    address_chain.clear();

    const auto expect_v1 = (config.version != PROXYProxyConfig::Version::v2);
    const auto expect_v2 = (config.version != PROXYProxyConfig::Version::v1);
    const auto header = tryReadPROXYHeader(socket, expect_v1, expect_v2);

    if (header.has_value())
        address_chain.emplace_back(header.value().source_address.to_string());
}

}
