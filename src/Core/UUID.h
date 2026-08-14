#pragma once

#include <Core/Types.h>

/**
 * Implementation Details
 * ^^^^^^^^^^^^^^^^^^^^^^
 * The underlying implementation for a UUID has it represented as a 128-bit unsigned integer. Underlying this, a wide
 * integer with a 64-bit unsigned integer as its base is utilized. This wide integer can be interfaced with as an array
 * to access different components of the base. For example, on a Little Endian platform, accessing at index 0 will give
 * you the 8 higher bytes, and index 1 will give you the 8 lower bytes. On a Big Endian platform, this is reversed where
 * index 0 will give you the 8 lower bytes, and index 1 will give you the 8 higher bytes.
 *
 *    uuid.toUnderType().items[0]
 *
 *    //  uint64_t   uint64_t
 *    // [xxxxxxxx] [        ]
 *
 *    uuid.toUnderType().items[1]
 *
 *    //  uint64_t   uint64_t
 *    // [        ] [xxxxxxxx]
 *
 * The way that data is stored in the underlying wide integer treats the data as two 64-bit chunks sequenced in the
 * array. On a Little Endian platform, this results in the following layout
 *
 *    // Suppose uuid contains 61f0c404-5cb3-11e7-907b-a6006ad3dba0
 *
 *    uuid.toUnderType().items[0]
 *
 *    //  uint64_t as HEX
 *    // [E7 11 B3 5C 04 C4 F0 61] [A0 DB D3 6A 00 A6 7B 90]
 *    //  ^^^^^^^^^^^^^^^^^^^^^^^
 *
 *    uuid.toUnderType().items[1]
 *
 *    //  uint64_t as HEX
 *    // [E7 11 B3 5C 04 C4 F0 61] [A0 DB D3 6A 00 A6 7B 90]
 *    //                            ^^^^^^^^^^^^^^^^^^^^^^^
 *
 * while on a Big Endian platform this would be
 *
 *    // Suppose uuid contains 61f0c404-5cb3-11e7-907b-a6006ad3dba0
 *
 *    uuid.toUnderType().items[0]
 *
 *    //  uint64_t as HEX
 *    // [90 7B A6 00 6A D3 DB A0] [61 F0 C4 04 5C B3 11 E7]
 *    //  ^^^^^^^^^^^^^^^^^^^^^^^
 *
 *    uuid.toUnderType().items[1]
 *
 *    //  uint64_t as HEX
 *    // [90 7B A6 00 6A D3 DB A0] [61 F0 C4 04 5C B3 11 E7]
 *    //                            ^^^^^^^^^^^^^^^^^^^^^^^
*/


namespace DB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    /// Generate UUID from hash of a string.
    UUID makeUUIDv4FromHash(const String & string);

    constexpr size_t HighBytes = (std::endian::native == std::endian::little) ? 0 : 1;
    constexpr size_t LowBytes = (std::endian::native == std::endian::little) ? 1 : 0;

    inline uint64_t getHighBytes(const UUID & uuid)
    {
        return uuid.toUnderType().items[HighBytes];
    }

    inline uint64_t & getHighBytes(UUID & uuid)
    {
        return uuid.toUnderType().items[HighBytes];
    }

    inline uint64_t getLowBytes(const UUID & uuid)
    {
        return uuid.toUnderType().items[LowBytes];
    }

    inline uint64_t & getLowBytes(UUID & uuid)
    {
        return uuid.toUnderType().items[LowBytes];
    }

    const UUID Nil{};
}

}
