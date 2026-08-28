#pragma once

#include <Columns/ColumnFixedString.h>
#include <Columns/IColumn.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>

namespace DB
{

/** Staging area for the bit planes of a block of rows on the way into a `QBit`'s `FixedString` columns.
  *
  * The natural way to build a `QBit` column is to append one `FixedString` row to every bit plane and
  * transpose straight into those rows. That makes every element of a row write into a different large
  * allocation at the same offset. The allocator hands out page-aligned pointers for allocations that
  * size, so those addresses agree in all the bits the L1 cache uses to pick a set, and the writes all
  * land in one set. Past the L1 associativity every one of them is a conflict miss: for
  * `QBit(Float64, 100)` this costs about 4x on the transposition, and it is only hidden today because
  * jemalloc's cache-oblivious padding offsets each large allocation by a random number of cache lines.
  *
  * Here the planes of a block are `BLOCK_ROWS * bytes_per_plane` apart inside one buffer. That is not
  * a multiple of the page size, so consecutive planes land in different sets and the conflicts go away
  * whatever the allocator does. `flush` then appends each plane to its column with a single sequential
  * copy.
  */
class QBitPlaneBlock
{
public:
    QBitPlaneBlock(size_t num_planes_, size_t bytes_per_plane_)
        : num_planes(num_planes_)
        , bytes_per_plane(bytes_per_plane_)
        , scratch(num_planes_ * bytes_per_plane_ * BLOCK_ROWS)
    {
    }

    /// Number of rows a single block holds. Chosen so that the scratch stays within L1 for the widest
    /// practical vectors, which is what keeps the transposition's writes local.
    static constexpr size_t BLOCK_ROWS = 32;

    /// Start a new block. The transposition ORs bits into the planes, so they start out zeroed.
    void start()
    {
        if (!scratch.empty())
            memset(scratch.data(), 0, scratch.size());
    }

    /// Start of plane `plane`'s bytes for row `row` of the current block.
    char * planeRow(size_t plane, size_t row) { return reinterpret_cast<char *>(scratch.data()) + plane * planeStride() + row * bytes_per_plane; }

    /// Append the first `rows` rows of every plane to the matching `FixedString` column.
    void flush(MutableColumns & columns, size_t rows)
    {
        for (size_t plane = 0; plane < num_planes; ++plane)
        {
            auto & chars = assert_cast<ColumnFixedString &>(*columns[plane]).getChars();
            const auto * from = scratch.data() + plane * planeStride();
            chars.insert(from, from + rows * bytes_per_plane);
        }
    }

private:
    size_t planeStride() const { return BLOCK_ROWS * bytes_per_plane; }

    size_t num_planes;
    size_t bytes_per_plane;
    VectorWithMemoryTracking<UInt8> scratch;
};

}
