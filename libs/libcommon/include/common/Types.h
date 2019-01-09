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

/// Workaround for the issue, that KDevelop doesn't see time_t and size_t types (for syntax highlight).
#ifdef IN_KDEVELOP_PARSER
    using time_t = Int64;
    using size_t = UInt64;
#endif
