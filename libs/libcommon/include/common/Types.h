#pragma once
#include <cstdint>
#include <cstddef>

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;


/** This is not the best way to overcome an issue of different definitions
  * of uint64_t and size_t on Linux and Mac OS X (both 64 bit).
  *
  * Note that on both platforms, long and long long are 64 bit types.
  * But they are always different types (with the same physical representation).
  */
namespace std
{
    inline UInt64 max(unsigned long x, unsigned long long y) { return x > y ? x : y; }
    inline UInt64 max(unsigned long long x, unsigned long y) { return x > y ? x : y; }
    inline UInt64 min(unsigned long x, unsigned long long y) { return x < y ? x : y; }
    inline UInt64 min(unsigned long long x, unsigned long y) { return x < y ? x : y; }

    inline Int64 max(long x, long long y) { return x > y ? x : y; }
    inline Int64 max(long long x, long y) { return x > y ? x : y; }
    inline Int64 min(long x, long long y) { return x < y ? x : y; }
    inline Int64 min(long long x, long y) { return x < y ? x : y; }
}


/// Workaround for the issue, that KDevelop doesn't see time_t and size_t types (for syntax highlight).
#ifdef IN_KDEVELOP_PARSER
    using time_t = Int64;
    using size_t = UInt64;
#endif
