#include <array>

namespace Poco { namespace Net { class IPAddress; }}

namespace DB
{

/// Convert IP address to 16-byte array with IPv6 data (big endian). If it's an IPv4, map it to IPv6.
std::array<char, 16> IPv6ToBinary(const Poco::Net::IPAddress & address);

}
