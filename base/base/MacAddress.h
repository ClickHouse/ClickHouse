#pragma once

#include <base/extended_types.h>
#include <base/strong_typedef.h>


namespace DB
{

/// MacAddress stored as 48-bit (6 bytes) value
/// Stored in network byte order (big-endian) for consistent comparison
struct MacAddress : StrongTypedef<UInt64, struct MacAddressTag>
{
    using StrongTypedef::StrongTypedef;
    using StrongTypedef::operator=;

    /// Default constructor creates zero MAC address
    constexpr MacAddress() : StrongTypedef(0) {}

    /// Construct from UInt64 (only lower 48 bits are used)
    constexpr explicit MacAddress(UInt64 value) : StrongTypedef(value & 0xFFFFFFFFFFFFULL) {}

    /// Get the underlying 48-bit value
    constexpr UInt64 toUInt64() const { return toUnderType() & 0xFFFFFFFFFFFFULL; }

    /// Comparison operators
    bool operator<(const MacAddress & rhs) const { return toUInt64() < rhs.toUInt64(); }
    bool operator>(const MacAddress & rhs) const { return toUInt64() > rhs.toUInt64(); }
    bool operator==(const MacAddress & rhs) const { return toUInt64() == rhs.toUInt64(); }
    bool operator<=(const MacAddress & rhs) const { return !operator>(rhs); }
    bool operator>=(const MacAddress & rhs) const { return !operator<(rhs); }
    bool operator!=(const MacAddress & rhs) const { return !operator==(rhs); }

    /// Check if this is a broadcast address (all FF)
    constexpr bool isBroadcast() const { return toUInt64() == 0xFFFFFFFFFFFFULL; }

    /// Check if this is a multicast address (least significant bit of first octet is 1)
    constexpr bool isMulticast() const { return (toUInt64() & 0x010000000000ULL) != 0; }

    /// Check if this is a unicast address
    constexpr bool isUnicast() const { return !isMulticast(); }

    /// Check if this is a locally administered address (second least significant bit of first octet is 1)
    constexpr bool isLocallyAdministered() const { return (toUInt64() & 0x020000000000ULL) != 0; }

    /// Check if this is a universally administered address
    constexpr bool isUniversallyAdministered() const { return !isLocallyAdministered(); }

    /// Check if this is a zero address
    constexpr bool isZero() const { return toUInt64() == 0; }
};

}

namespace std
{
    template <>
    struct hash<DB::MacAddress>
    {
        size_t operator()(const DB::MacAddress & x) const
        {
            return std::hash<DB::MacAddress::UnderlyingType>()(x.toUnderType());
        }
    };
}

