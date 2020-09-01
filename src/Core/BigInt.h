#pragma once

#include <common/StringRef.h>
#include <Core/Types.h>

namespace DB
{

template <typename T>
struct BigInt
{
    static_assert(sizeof(T) == 32);
    static constexpr size_t size = 32;

    static StringRef serialize(const T & x, char * pos)
    {
        //unalignedStore<T>(pos, x);
        memcpy(pos, &x, size);
        return StringRef(pos, size);
    }

    static String serialize(const T & x)
    {
        String str(size, '\0');
        serialize(x, str.data());
        return str;
    }

    static T deserialize(const char * pos)
    {
        //return unalignedLoad<T>(pos);
        T res;
        memcpy(&res, pos, size);
        return res;
    }

    static std::vector<UInt64> toIntArray(const T &)
    {
        /// FIXME
        std::vector<UInt64> parts(4, 0);
        return parts;
    }
};

}
