#pragma once

#include <base/extended_types.h>
#include <base/strong_typedef.h>
#include <base/unaligned.h>

#include <bit>


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

        /// Ordered by the network byte representation, not by the little-endian UInt128 value.
        /// Comparing the byte-swapped halves as one native 128-bit integer is branchless (cmp + sbb),
        /// which is faster than memcmp16 when it is not predictable whether the values are equal.
        bool operator<(const IPv6 & rhs) const { return asBigEndian() < rhs.asBigEndian(); }
        bool operator>(const IPv6 & rhs) const { return asBigEndian() > rhs.asBigEndian(); }
        bool operator==(const IPv6 & rhs) const { return toUnderType() == rhs.toUnderType(); }

        bool operator<=(const IPv6 & rhs) const { return !operator>(rhs); }
        bool operator>=(const IPv6 & rhs) const { return !operator<(rhs); }
        bool operator!=(const IPv6 & rhs) const { return !operator==(rhs); }

    private:
        unsigned __int128 asBigEndian() const
        {
            UInt64 hi = unalignedLoad<UInt64>(&toUnderType());
            UInt64 lo = unalignedLoad<UInt64>(reinterpret_cast<const char *>(&toUnderType()) + sizeof(hi));
            if constexpr (std::endian::native == std::endian::little)
            {
                hi = std::byteswap(hi);
                lo = std::byteswap(lo);
            }
            return static_cast<unsigned __int128>(hi) << 64 | lo;
        }
    };

}

namespace std
{
    /// For historical reasons we hash IPv6 as a FixedString(16)
    template <>
    struct hash<DB::IPv6>
    {
        size_t operator()(const DB::IPv6 & x) const
        {
            return std::hash<std::string_view>{}(
                std::string_view(reinterpret_cast<const char *>(&x.toUnderType()), sizeof(DB::IPv6::UnderlyingType)));
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
