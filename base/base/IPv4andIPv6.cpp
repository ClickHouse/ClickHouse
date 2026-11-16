#include <base/IPv4andIPv6.h>

#include <base/memcmpSmall.h>

namespace DB
{

bool IPv6::operator<(const IPv6 & rhs) const
{
    return
        memcmp16(
            reinterpret_cast<const unsigned char *>(toUnderType().items),
            reinterpret_cast<const unsigned char *>(rhs.toUnderType().items)
        ) < 0;
}

bool IPv6::operator>(const IPv6 & rhs) const
{
    return
        memcmp16(
            reinterpret_cast<const unsigned char *>(toUnderType().items),
            reinterpret_cast<const unsigned char *>(rhs.toUnderType().items)
        ) > 0;
}

bool IPv6::operator==(const IPv6 & rhs) const
{
    return
        memcmp16(
            reinterpret_cast<const unsigned char *>(toUnderType().items),
            reinterpret_cast<const unsigned char *>(rhs.toUnderType().items)
        ) == 0;
}

}
