#pragma once

#include <Core/Types.h>


namespace DB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    const size_t HighBytes =
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        1;
#else
        0;
#endif

    const size_t LowBytes =
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
        0;
#else
        1;
#endif

    inline void toLegacyFormat(UUID & uuid)
    {
        // WARNING: This function should only be used for maintaining backwards compatibility to convert a UUID from its
        // correct byte ordering implementation to the previous implementation during various forms of processing where
        // the individual byte ordering is important. This can include serialization, deserialization, reinterpretation,
        // hashing, etc.
        auto & impl = uuid.toUnderType();
        std::swap(impl.items[HighBytes], impl.items[LowBytes]);
    }

    inline uint64_t getUUIDHigh(const UUID & uuid)
    {
        return uuid.toUnderType().items[HighBytes];
    }

    inline uint64_t & getUUIDHigh(UUID & uuid)
    {
        return uuid.toUnderType().items[HighBytes];
    }

    inline uint64_t getUUIDLow(const UUID & uuid)
    {
        return uuid.toUnderType().items[LowBytes];
    }

    inline uint64_t & getUUIDLow(UUID & uuid)
    {
        return uuid.toUnderType().items[LowBytes];
    }

    const UUID Nil{};
}

}
