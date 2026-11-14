#include <Storages/Statistics/DistinctValuesFromCompressedSize.h>

#include <algorithm>
#include <cmath>

namespace DB
{

namespace
{

/// See the comment in the header.
constexpr double max_compressed_bits_per_row = 10;

}

std::optional<UInt64> estimateDistinctValuesFromCompressedSize(UInt64 rows, UInt64 compressed_bytes)
{
    if (rows == 0)
        return std::nullopt;

    const double bits_per_row = 8.0 * static_cast<double>(compressed_bytes) / static_cast<double>(rows);
    if (!(bits_per_row < max_compressed_bits_per_row))
        return std::nullopt;

    const auto distinct_values = static_cast<UInt64>(std::llround(std::exp2(bits_per_row)));
    return std::clamp<UInt64>(distinct_values, 1, rows);
}

}
