#pragma once

#include <Core/UUID.h>

#include <algorithm>
#include <bit>
#include <cstring>

namespace DB::Parquet
{

inline void transformParquetUUIDByteOrder(UUID & value)
{
    auto * bytes = reinterpret_cast<UInt8 *>(&value);

    /// `Parquet` stores `UUID` values in big-endian network byte order, while
    /// ClickHouse stores them as two native-order 64-bit words.
    if constexpr (std::endian::native == std::endian::little)
    {
        std::reverse(bytes, bytes + 8);
        std::reverse(bytes + 8, bytes + 16);
    }
    else
    {
        std::swap_ranges(bytes, bytes + 8, bytes + 8);
    }
}

inline UUID decodeParquetUUID(const char * data)
{
    UUID result;
    std::memcpy(&result, data, sizeof(result));
    transformParquetUUIDByteOrder(result);
    return result;
}

inline UUID decodeParquetUUID(const UInt8 * data)
{
    return decodeParquetUUID(reinterpret_cast<const char *>(data));
}

inline UUID encodeParquetUUID(UUID value)
{
    transformParquetUUIDByteOrder(value);
    return value;
}

}
