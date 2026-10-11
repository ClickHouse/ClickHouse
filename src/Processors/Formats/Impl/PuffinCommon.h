#pragma once

#include <base/types.h>

#include <limits>

namespace DB
{

inline constexpr UInt8 PUFFIN_MAGIC[4] = {0x50, 0x46, 0x41, 0x31};
inline constexpr UInt8 DELETION_VECTOR_MAGIC[4] = {0xD1, 0xD3, 0x39, 0x64};
inline constexpr Int64 DELETION_VECTOR_MAX_POSITION = 0x7FFFFFFE80000000LL;
inline constexpr Int32 DELETION_VECTOR_MAX_KEY = std::numeric_limits<Int32>::max() - 1;
inline constexpr const char * PUFFIN_DELETION_VECTOR_BLOB_TYPE = "deletion-vector-v1";

}
