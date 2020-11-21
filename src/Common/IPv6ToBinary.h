#pragma once
#include <array>
#include <common/types.h>

namespace Poco { namespace Net { class IPAddress; }}

namespace DB
{

/// Convert IP address to raw binary with IPv6 data (big endian). If it's an IPv4, map it to IPv6.
/// Saves result into the first 16 bytes of `res`.
void IPv6ToRawBinary(const Poco::Net::IPAddress & address, char * res);

/// Convert IP address to 16-byte array with IPv6 data (big endian). If it's an IPv4, map it to IPv6.
std::array<char, 16> IPv6ToBinary(const Poco::Net::IPAddress & address);

}
