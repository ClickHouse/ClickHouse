#pragma once

#include <Core/Defines.h>
#include <IO/WriteBuffer.h>
#include <base/itoa.h>
#include <base/wide_integer_to_string.h>


template <typename T> constexpr size_t max_int_width = 20;
template <> inline constexpr size_t max_int_width<UInt8> = 3;    /// 255
template <> inline constexpr size_t max_int_width<Int8> = 4;     /// -128
template <> inline constexpr size_t max_int_width<UInt16> = 5;   /// 65535
template <> inline constexpr size_t max_int_width<Int16> = 6;    /// -32768
template <> inline constexpr size_t max_int_width<UInt32> = 10;  /// 4294967295
template <> inline constexpr size_t max_int_width<Int32> = 11;   /// -2147483648
template <> inline constexpr size_t max_int_width<UInt64> = 20;  /// 18446744073709551615
template <> inline constexpr size_t max_int_width<Int64> = 20;   /// -9223372036854775808
template <> inline constexpr size_t max_int_width<UInt128> = 39; /// 340282366920938463463374607431768211455
template <> inline constexpr size_t max_int_width<Int128> = 40;  /// -170141183460469231731687303715884105728
template <> inline constexpr size_t max_int_width<UInt256> = 78; /// 115792089237316195423570985008687907853269984665640564039457584007913129639935
template <> inline constexpr size_t max_int_width<Int256> = 78;  /// -57896044618658097711785492504343953926634992332820282019728792003956564819968
template <> inline constexpr size_t max_int_width<UInt512> = 155; /// 512-bit decimal digits
template <> inline constexpr size_t max_int_width<Int512> = 155;  /// 512-bit decimal digits


namespace DB
{

namespace detail
{
    template <typename T>
    char * writeUIntText(T x, char * pos)
    {
        if (x == 0)
        {
            *pos = '0';
            return pos + 1;
        }

        char tmp[max_int_width<T>];
        char * end = tmp + max_int_width<T>;
        while (x != 0)
        {
            *--end = '0' + x % 10;
            x /= 10;
        }
        memcpy(pos, end, tmp + max_int_width<T> - end);
        return pos + (tmp + max_int_width<T> - end);
    }

    template <typename T>
    void NO_INLINE writeUIntTextFallback(T x, WriteBuffer & buf)
    {
        char tmp[max_int_width<T>];
        char * end = itoa(x, tmp);
        buf.write(tmp, end - tmp);
    }
}

template <typename T>
void writeIntText(T x, WriteBuffer & buf)
{
    if constexpr (is_big_int_v<T>)
    {
        writeString(wide::to_string(x), buf);
    }
    else
    {
        if (likely(buf.available() >= 64))
        {
            buf.position() = detail::writeUIntText(x, buf.position());
        }
        else
        {
            detail::writeUIntTextFallback(x, buf);
        }
    }
}

}
