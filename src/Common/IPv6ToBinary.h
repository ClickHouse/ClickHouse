#pragma once
#include <array>
#include <common/types.h>

namespace Poco { namespace Net { class IPAddress; }}

namespace DB
{

/// Convert IP address to 16-byte array with IPv6 data (big endian). If it's an IPv4, map it to IPv6.
std::array<char, 16> IPv6ToBinary(const Poco::Net::IPAddress & address);

/// Convert IP address to UInt32 (big endian) if it's IPv4 or IPv4 mapped to IPv6.
/// Sets success variable to true if succeed.
UInt32 IPv4ToBinary(const Poco::Net::IPAddress & address, bool & success);

}
