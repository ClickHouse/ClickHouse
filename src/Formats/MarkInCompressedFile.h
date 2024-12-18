#pragma once

#include <tuple>

#include <base/types.h>
#include <Common/PODArray.h>


namespace DB
{

/** Mark is the position in the compressed file. The compressed file consists of adjacent compressed blocks.
  * Mark is a tuple - the offset in the file to the start of the compressed block, the offset in the decompressed block to the start of the data.
  */
struct MarkInCompressedFile
{
    size_t offset_in_compressed_file;
    size_t offset_in_decompressed_block;

    auto operator<=>(const MarkInCompressedFile &) const = default;

    auto asTuple() const { return std::make_tuple(offset_in_compressed_file, offset_in_decompressed_block); }

    String toString() const;
    String toStringWithRows(size_t rows_num) const;
};

/**
 * In-memory representation of an array of marks.
 *
 * Uses an ad-hoc compression scheme that decreases memory usage while allowing
 * random access in O(1) time.
 * This is independent from the marks *file* format, which may be uncompressed
 * or use a different compression method.
 *
 * Typical memory usage:
 *  * ~3 bytes/mark for integer columns
 *  * ~5 bytes/mark for string columns
 *  * ~0.3 bytes/mark for trivial marks in auxiliary dict files of LowCardinality columns
 */
class MarksInCompressedFile
{
public:
    using PlainArray = PODArray<MarkInCompressedFile>;

    explicit MarksInCompressedFile(const PlainArray & marks);

    MarkInCompressedFile get(size_t idx) const;

    size_t approximateMemoryUsage() const;

private:
    /** Throughout this class:
     *   * "x" stands for offset_in_compressed_file,
     *   * "y" stands for offset_in_decompressed_block.
     */

    /** We need to store a sequence of marks, each consisting of two 64-bit integers:
     * offset_in_compressed_file and offset_in_decompressed_block. We'll call them x and y for
     * convenience, since compression doesn't care what they mean. The compression exploits the
     * following regularities:
     *  * y is usually zero.
     *  * x usually increases steadily.
     *  * Differences between x values in nearby marks usually fit in much fewer than 64 bits.
     *
     * We split the sequence of marks into blocks, each containing MARKS_PER_BLOCK marks.
     * (Not to be confused with data blocks.)
     * For each mark, we store the difference [value] - [min value in the block], for each of the
     * two values in the mark. Each block specifies the number of bits to use for these differences
     * for all marks in this block.
     * The smaller the blocks the fewer bits are required, but the bigger the relative overhead of
     * block headers.
     *
     * Packed marks and block headers all live in one contiguous array.
     */

    struct BlockInfo
    {
        // Min offset_in_compressed_file and offset_in_decompressed_block, correspondingly.
        size_t min_x = UINT64_MAX;
        size_t min_y = UINT64_MAX;

        // Place in `packed` where this block start.
        size_t bit_offset_in_packed_array;

        // How many bits each mark takes. These numbers are bit-packed in the `packed` array.
        // Can be zero. (Especially for y, which is typically all zeroes.)
        UInt8 bits_for_x;
        UInt8 bits_for_y;
        // The `y` values should be <<'ed by this amount.
        // Useful for integer columns when marks granularity is a power of 2; in this case all
        // offset_in_decompressed_block values are divisible by 2^15 or so.
        UInt8 trailing_zero_bits_in_y = 63;
    };

    static constexpr size_t MARKS_PER_BLOCK = 256;

    size_t num_marks;
    PODArray<BlockInfo> blocks;
    PODArray<UInt64> packed;

    // Mark idx -> {block info, bit offset in `packed`}.
    std::tuple<const BlockInfo *, size_t> lookUpMark(size_t idx) const;
};

using PlainMarksByName = std::unordered_map<String, std::unique_ptr<MarksInCompressedFile::PlainArray>>;

}
