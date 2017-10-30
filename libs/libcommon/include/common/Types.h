#pragma once

#include <Poco/Types.h>

using Int8 = Poco::Int8;
using Int16 = Poco::Int16;
using Int32 = Poco::Int32;
using Int64 = Poco::Int64;

using UInt8 = Poco::UInt8;
using UInt16 = Poco::UInt16;
using UInt32 = Poco::UInt32;
using UInt64 = Poco::UInt64;


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
