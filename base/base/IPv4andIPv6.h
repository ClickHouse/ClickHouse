#pragma once

#include <base/strong_typedef.h>
#include <base/extended_types.h>
#include <Common/memcmpSmall.h>

namespace DB
{

    using IPv4 = StrongTypedef<UInt32, struct IPv4Tag>;

    struct IPv6 : StrongTypedef<UInt128, struct IPv6Tag>
    {
        constexpr IPv6() = default;
        constexpr explicit IPv6(const UInt128 & x) : StrongTypedef(x) {}
        constexpr explicit IPv6(UInt128 && x) : StrongTypedef(std::move(x)) {}

        IPv6 & operator=(const UInt128 & rhs) { StrongTypedef::operator=(rhs); return *this; }
        IPv6 & operator=(UInt128 && rhs) { StrongTypedef::operator=(std::move(rhs)); return *this; }

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
}
