#pragma once

#include <base/strong_typedef.h>
#include <base/extended_types.h>
#include <Common/memcmpSmall.h>

namespace DB
{

    struct IPv4 : StrongTypedef<UInt32, struct IPv4Tag>
    {
        using StrongTypedef::StrongTypedef;
        using StrongTypedef::operator=;
        constexpr explicit IPv4(UInt64 value): StrongTypedef(static_cast<UnderlyingType>(value)) {}
    };

    struct IPv6 : StrongTypedef<UInt128, struct IPv6Tag>
    {
        using StrongTypedef::StrongTypedef;
        using StrongTypedef::operator=;

        bool operator<(const IPv6 & rhs) const
        {
            return
                memcmp16(
                    reinterpret_cast<const unsigned char *>(toUnderType().items),
                    reinterpret_cast<const unsigned char *>(rhs.toUnderType().items)
                ) < 0;
        }

        bool operator>(const IPv6 & rhs) const
        {
            return
                memcmp16(
                    reinterpret_cast<const unsigned char *>(toUnderType().items),
                    reinterpret_cast<const unsigned char *>(rhs.toUnderType().items)
                ) > 0;
        }

        bool operator==(const IPv6 & rhs) const
        {
            return
                memcmp16(
                    reinterpret_cast<const unsigned char *>(toUnderType().items),
                    reinterpret_cast<const unsigned char *>(rhs.toUnderType().items)
                ) == 0;
        }

        bool operator<=(const IPv6 & rhs) const { return !operator>(rhs); }
        bool operator>=(const IPv6 & rhs) const { return !operator<(rhs); }
        bool operator!=(const IPv6 & rhs) const { return !operator==(rhs); }
    };

}

namespace std
{
    template <>
    struct hash<DB::IPv6>
    {
        size_t operator()(const DB::IPv6 & x) const
        {
            return std::hash<DB::IPv6::UnderlyingType>()(x.toUnderType());
        }
    };

    template <>
    struct hash<DB::IPv4>
    {
        size_t operator()(const DB::IPv4 & x) const
        {
            return std::hash<DB::IPv4::UnderlyingType>()(x.toUnderType());
        }
    };
}
