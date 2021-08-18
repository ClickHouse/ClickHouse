#pragma once

#include <cstring>
#include <cassert>

#include <Columns/IColumn.h>
#include <Common/PODArray.h>
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

/** Simplifies writing data to the ColumnString or ColumnFixedString via WriteBuffer.
 *
 *  Take care of little subtle details, like padding or proper offsets.
 */
template <typename ColumnType>
class WriteHelper
{
    ColumnType & col;
    WriteBufferFromVector<typename ColumnType::Chars> buffer;
    size_t prev_row_buffer_size = 0;

public:
    WriteHelper(ColumnType & col_, size_t expected_rows, size_t expected_row_size [[maybe_unused]] = 0)
        : col(col_),
          buffer(col.getChars())
    {
        if constexpr (std::is_same_v<ColumnType, ColumnFixedString>)
            col.reserve(expected_rows);
        else
        {
            if (const size_t estimated_total_size = expected_rows * expected_row_size)
                col.reserve(estimated_total_size);
        }
    }

    ~WriteHelper()
    {
        if (buffer.count())
            rowWritten();

        buffer.finalize();
    }

    auto & getWriteBuffer()
    {
        return buffer;
    }

    inline void rowWritten()
    {
        if constexpr (std::is_same_v<ColumnType, ColumnFixedString>)
        {
            if (buffer.count() > prev_row_buffer_size + col.getN())
                throw Exception(
                        ErrorCodes::TOO_LARGE_STRING_SIZE,
                        "Too large string for FixedString column");

            // Pad with zeroes on the right to maintain FixedString invariant.
            const auto excess_bytes = buffer.count() % col.getN();
            const auto fill_bytes = col.getN() - excess_bytes;
            for (size_t i = 0; i < fill_bytes; ++i)
                buffer.write('\0');
        }
        else
        {
            writeChar(0, buffer);
            col.getOffsets().push_back(buffer.count());
        }

        prev_row_buffer_size = buffer.count();
    }
};

}

}
