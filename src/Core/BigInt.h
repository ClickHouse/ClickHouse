#pragma once

#include <common/StringRef.h>
#include <Core/Types.h>

namespace DB
{

template <typename T>
struct BigIntPayload
{
    static_assert(!is_big_int_v<T>);
    static constexpr size_t size = 0;
};

template <> struct BigIntPayload<bUInt256> { static constexpr size_t size = 32; };

template <> struct BigIntPayload<bInt256>
{
    using UnsingedType = bUInt256;
    static constexpr size_t size = 32;
};

template <typename T>
struct BigInt : BigIntPayload<T>
{
    using BigIntPayload<T>::size;

    static constexpr size_t lastBit()
    {
        return size * 8 - 1;
    }

    static StringRef serialize(const T & x, char * pos)
    {
        if constexpr (is_signed_v<T>)
        {
            using UnsignedT = typename BigIntPayload<T>::UnsingedType;

            if (x < 0)
            {
                UnsignedT unsigned_x = UnsignedT{0} - static_cast<UnsignedT>(-x);
                export_bits(unsigned_x, pos, 8, false);
            }
            else
                export_bits(x, pos, 8, false);
        }
        else
            export_bits(x, pos, 8, false);
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
        if constexpr (is_signed_v<T>)
        {
            using UnsignedT = typename BigIntPayload<T>::UnsingedType;

            UnsignedT unsigned_x;
            import_bits(unsigned_x, pos, pos + size, false);

            bool is_negative = bit_test(unsigned_x, lastBit());
            if (is_negative)
                unsigned_x = UnsignedT{0} - unsigned_x;
            return static_cast<T>(unsigned_x);
        }
        else
        {
            T x;
            import_bits(x, pos, pos + size, false);
            return x;
        }
    }
};

}
