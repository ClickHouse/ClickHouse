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

    static std::vector<UInt64> toIntArray(const T & x)
    {
        std::vector<UInt64> parts;
        export_bits(x, std::back_inserter(parts), sizeof(UInt64), false);
        return parts;
    }
};

template <>
struct BigInt<wUInt256>
{
    static_assert(sizeof(wUInt256) == 32);
    static constexpr size_t size = 32;

    static StringRef serialize(const wUInt256 & x, char * pos)
    {
        //unalignedStore<wUInt256>(pos, x);
        memcpy(pos, &x, size);
        return StringRef(pos, size);
    }

    static String serialize(const wUInt256 & x)
    {
        String str(size, '\0');
        serialize(x, str.data());
        return str;
    }

    static wUInt256 deserialize(const char * pos)
    {
        //return unalignedLoad<wUInt256>(pos);
        wUInt256 res;
        memcpy(&res, pos, size);
        return res;
    }

    static std::vector<UInt64> toIntArray(const wUInt256 &)
    {
        /// FIXME
        std::vector<UInt64> parts(4, 0);
        return parts;
    }
};

}
