#pragma once

#include <cstring>

#include <Common/Exception.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <IO/WriteBufferFromVector.h>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LARGE_STRING_SIZE;
}

namespace ColumnStringHelpers
{

/** Simplifies writing data to a ColumnString or ColumnFixedString via WriteBuffer.
  * Takes care of little subtle details, like proper offsets and padding.
  */
template <typename ColumnType>
class WriteHelper
{
    ColumnType & col;
    WriteBufferFromVector<typename ColumnType::Chars> buffer;
    size_t prev_row_buffer_size = 0;

    static ColumnType & resizeColumn(ColumnType & column, size_t rows)
    {
        if constexpr (std::is_same_v<ColumnType, ColumnFixedString>)
        {
            column.resize(rows);
        }
        else
        {
            column.getOffsets().reserve_exact(rows);
            /// The usage of coefficient 2 for initial size is arbitrary.
            column.getChars().reserve_exact(rows * 2);
        }
        return column;
    }

public:
    WriteHelper(ColumnType & col_, size_t expected_rows)
        : col(resizeColumn(col_, expected_rows))
        , buffer(col.getChars())
    {
    }

    ~WriteHelper() = default;

    void finalize()
    {
        buffer.finalize();
    }

    auto & getWriteBuffer()
    {
        return buffer;
    }

    void finishRow()
    {
        if constexpr (std::is_same_v<ColumnType, ColumnFixedString>)
        {
            if (buffer.count() > prev_row_buffer_size + col.getN())
                throw Exception(
                        ErrorCodes::TOO_LARGE_STRING_SIZE,
                        "Too large string for FixedString column");

            // Pad with zeroes on the right to maintain FixedString invariant.
            if (buffer.count() % col.getN() != 0 || buffer.count() == prev_row_buffer_size)
            {
                const auto excess_bytes = buffer.count() % col.getN();
                const auto fill_bytes = col.getN() - excess_bytes;
                writeChar(0, fill_bytes, buffer);
            }
        }
        else
        {
            col.getOffsets().push_back(buffer.count());
        }

        prev_row_buffer_size = buffer.count();
    }
};

/// Reserve capacity for a chars buffer that is grown incrementally (one value per call). Keeps the
/// default power-of-two doubling (amortized O(1) appends) until a single growth increment would exceed
/// `max_growth_step`, after which it grows by exact step-sized chunks. This bounds the over-allocation
/// for large buffers without reallocating on every row, which a plain per-row `reserve_exact` would
/// cause. `max_growth_step == 0` keeps pure power-of-two growth.
inline void reserveCharsWithGrowthCap(ColumnString::Chars & chars, size_t required, size_t max_growth_step)
{
    if (required <= chars.capacity())
        return;

    if (max_growth_step == 0)
    {
        chars.reserve(required);
        return;
    }

    size_t new_capacity = chars.capacity() * 2;
    if (new_capacity - chars.capacity() > max_growth_step)
        new_capacity = chars.capacity() + max_growth_step;
    chars.reserve_exact(std::max(new_capacity, required));
}

}

}
