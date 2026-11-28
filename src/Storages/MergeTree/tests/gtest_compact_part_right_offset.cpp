#include <gtest/gtest.h>

#include <Formats/MarkInCompressedFile.h>
#include <Storages/MergeTree/MergeTreeMarksLoader.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

#include <fmt/format.h>

#include <algorithm>
#include <map>
#include <optional>
#include <random>

using namespace DB;

namespace
{

/// Writes one piece of data per granule and mark position into compressed blocks, like the Compact part writer,
/// and remembers the marks and where each piece ends.
class CompactFileSimulator
{
public:
    /// `first_position_of_column` maps a position to the first position of its column.
    CompactFileSimulator(size_t num_granules_, size_t num_positions_, size_t max_block_size_, std::vector<size_t> first_position_of_column_)
        : num_granules(num_granules_)
        , num_positions(num_positions_)
        , max_block_size(max_block_size_)
        , first_position_of_column(std::move(first_position_of_column_))
    {
        marks.resize_fill((num_granules + 1) * num_positions);
        piece_ends.resize(num_granules * num_positions);
    }

    void writePiece(size_t granule, size_t position, size_t size)
    {
        marks[granule * num_positions + position] = {.offset_in_compressed_file = compressed_offset, .offset_in_decompressed_block = block_size};
        while (size)
        {
            size_t added = std::min(size, max_block_size - block_size);
            block_size += added;
            size -= added;
            if (block_size == max_block_size)
                cutBlock();
        }
        piece_ends[granule * num_positions + position] = {.offset_in_compressed_file = compressed_offset, .offset_in_decompressed_block = block_size};
    }

    void cutBlock()
    {
        if (!block_size)
            return;

        size_t compressed_size = block_size / 2 + 25;
        block_ends[compressed_offset] = compressed_offset + compressed_size;
        compressed_offset += compressed_size;
        block_size = 0;
    }

    /// Cuts the last block and writes the final mark, which points to the end of the file.
    void finish()
    {
        cutBlock();
        for (size_t position = 0; position < num_positions; ++position)
            marks[num_granules * num_positions + position] = {.offset_in_compressed_file = compressed_offset, .offset_in_decompressed_block = 0};
    }

    [[nodiscard]] size_t getFileSize() const { return compressed_offset; }
    [[nodiscard]] size_t getMarksCount() const { return num_granules + 1; }
    [[nodiscard]] size_t getFirstPositionOfColumn(size_t position) const { return first_position_of_column[position]; }
    [[nodiscard]] MergeTreeMarksGetter getMarksGetter() const { return MergeTreeMarksGetter(MarksInCompressedFile::create(marks), num_positions); }

    /// The end of the block with the last byte of the piece.
    [[nodiscard]] size_t getNeededEnd(size_t granule, size_t position) const
    {
        const auto & end = piece_ends[granule * num_positions + position];
        if (end.offset_in_decompressed_block == 0)
            return end.offset_in_compressed_file;
        return block_ends.at(end.offset_in_compressed_file);
    }

    /// The right offset by a scan of all marks, which is what the binary searches must find.
    [[nodiscard]] size_t getRightOffsetByAllMarks(size_t right_mark_non_included, size_t position) const
    {
        if (right_mark_non_included == 0)
            return marks[position].offset_in_compressed_file;

        auto first_greater = [&](const MarkInCompressedFile & value)
        {
            std::optional<MarkInCompressedFile> result;
            for (const auto & mark : marks)
            {
                if (mark > value && (!result || mark < *result))
                    result = mark;
            }
            return result;
        };

        size_t last_granule = right_mark_non_included - 1;
        auto next = first_greater(marks[last_granule * num_positions + position]);

        /// The cells that always follow (`last_granule`, `position`) in the file, whose marks count even when they are equal.
        auto take = [&](size_t granule, size_t next_position)
        {
            const auto & mark = marks[granule * num_positions + next_position];
            if (!next || mark < *next)
                next = mark;
        };

        if (position + 1 < num_positions)
            take(last_granule, position + 1);
        if (last_granule + 1 < getMarksCount())
            take(last_granule + 1, first_position_of_column[position]);

        if (!next)
            return getFileSize();

        if (next->offset_in_decompressed_block == 0)
            return next->offset_in_compressed_file;

        auto after_block = first_greater({.offset_in_compressed_file = next->offset_in_compressed_file, .offset_in_decompressed_block = std::numeric_limits<size_t>::max()});
        return after_block ? after_block->offset_in_compressed_file : getFileSize();
    }

private:
    const size_t num_granules;
    const size_t num_positions;
    const size_t max_block_size;
    const std::vector<size_t> first_position_of_column;

    MarksInCompressedFile::PlainArray marks;
    std::vector<MarkInCompressedFile> piece_ends;
    std::map<size_t, size_t> block_ends;

    size_t compressed_offset = 0;
    size_t block_size = 0;
};

/// Where the writer cuts compressed blocks, besides the maximum block size.
enum class Cut
{
    /// A new block for each column in each granule (`compress_per_column_in_compact_parts = 1`).
    PerColumnInGranule,
    /// A new block for each substream too, as when the substreams of a column use different codecs.
    PerSubstreamInGranule,
    /// A new block for each stripe (`compress_per_column_in_compact_parts = 0`).
    PerStripe,
    /// A new block for each column of a stripe. No writer does it, but the bound must still be safe.
    PerColumnInStripe,
    /// Blocks are cut only when they reach the maximum size.
    BySizeOnly,
};

struct LayoutParams
{
    Cut cut;
    /// In each stripe, each column for all granules of the stripe. With 1 all columns of a granule follow each other.
    size_t granules_in_stripe;
    size_t max_block_size;
    /// The searches find the real next mark. Otherwise they may find a later one, which is still a safe bound.
    bool exact;
};

/// Substreams of one column share a block, like the null map and the values of a `Nullable` column.
const std::vector<size_t> substreams_in_columns = {1, 2, 1, 3, 1, 2};
constexpr size_t num_granules = 10;

CompactFileSimulator writeFile(const LayoutParams & params, std::mt19937 & rng)
{
    size_t num_positions = 0;
    std::vector<size_t> first_positions;
    for (size_t substreams : substreams_in_columns)
    {
        first_positions.push_back(num_positions);
        num_positions += substreams;
    }

    /// Small pieces, ones larger than a block, and empty ones (elements of empty arrays). The first substream
    /// of a column is never empty, as a granule always has rows.
    std::vector<size_t> sizes(num_granules * num_positions);
    for (size_t granule = 0; granule < num_granules; ++granule)
    {
        for (size_t position = 0; position < num_positions; ++position)
        {
            bool is_first_substream = std::find(first_positions.begin(), first_positions.end(), position) != first_positions.end();
            size_t kind = rng() % 6;
            auto & size = sizes[granule * num_positions + position];
            if (kind == 0 && !is_first_substream)
                size = 0;
            else if (kind == 5)
                size = params.max_block_size * 2 + rng() % 1000;
            else
                size = 1 + rng() % 3000;
        }
    }

    std::vector<size_t> first_position_of_column;
    for (size_t column = 0; column < substreams_in_columns.size(); ++column)
        first_position_of_column.insert(first_position_of_column.end(), substreams_in_columns[column], first_positions[column]);

    CompactFileSimulator file(num_granules, num_positions, params.max_block_size, std::move(first_position_of_column));

    auto write_column = [&](size_t granule, size_t column)
    {
        for (size_t i = 0; i < substreams_in_columns[column]; ++i)
        {
            size_t position = first_positions[column] + i;
            file.writePiece(granule, position, sizes[granule * num_positions + position]);
            if (params.cut == Cut::PerSubstreamInGranule)
                file.cutBlock();
        }
        if (params.cut == Cut::PerColumnInGranule)
            file.cutBlock();
    };

    for (size_t stripe_begin = 0; stripe_begin < num_granules; stripe_begin += params.granules_in_stripe)
    {
        size_t stripe_end = std::min(num_granules, stripe_begin + params.granules_in_stripe);
        for (size_t column = 0; column < substreams_in_columns.size(); ++column)
        {
            for (size_t granule = stripe_begin; granule < stripe_end; ++granule)
                write_column(granule, column);
            if (params.cut == Cut::PerColumnInStripe)
                file.cutBlock();
        }
        if (params.cut == Cut::PerStripe)
            file.cutBlock();
    }

    file.finish();
    return file;
}

}

TEST(CompactPartRightOffset, MatchesScanOfAllMarks)
{
    /// For stripes with blocks that hold several granules of a column only the bound's safety is checked.
    const std::vector<LayoutParams> layouts = {
        {Cut::PerColumnInGranule, 1, 1 << 20, true},
        {Cut::PerColumnInGranule, 1, 4096, true},
        {Cut::PerColumnInGranule, 3, 1 << 20, true},
        {Cut::PerColumnInGranule, 4, 4096, true},
        {Cut::PerSubstreamInGranule, 1, 1 << 20, true},
        {Cut::PerSubstreamInGranule, 1, 4096, true},
        {Cut::PerSubstreamInGranule, 4, 1 << 20, false},
        {Cut::PerSubstreamInGranule, 4, 4096, false},
        {Cut::PerStripe, 1, 1 << 20, true},
        {Cut::PerStripe, 1, 4096, true},
        {Cut::PerStripe, 4, 1 << 20, false},
        {Cut::PerStripe, 4, 4096, false},
        {Cut::PerColumnInStripe, 4, 1 << 20, false},
        {Cut::BySizeOnly, 1, 4096, true},
        {Cut::BySizeOnly, 4, 4096, false},
    };

    for (size_t layout_index = 0; layout_index < layouts.size(); ++layout_index)
    {
        for (size_t seed = 0; seed < 20; ++seed)
        {
            SCOPED_TRACE(fmt::format("layout {}, seed {}", layout_index, seed));
            std::mt19937 rng(static_cast<unsigned>(seed)); /// NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp) reproducible failures

            auto file = writeFile(layouts[layout_index], rng);
            auto marks = file.getMarksGetter();

            for (size_t right_mark = 0; right_mark <= file.getMarksCount(); ++right_mark)
            {
                for (size_t position = 0; position < marks.getNumColumns(); ++position)
                {
                    SCOPED_TRACE(fmt::format("right mark {}, position {}", right_mark, position));

                    size_t offset = MergeTreeReaderStreamMultipleColumns::getRightOffsetByMarks(
                        marks, file.getMarksCount(), file.getFileSize(), right_mark, position, file.getFirstPositionOfColumn(position));

                    size_t by_all_marks = file.getRightOffsetByAllMarks(right_mark, position);
                    if (layouts[layout_index].exact)
                        EXPECT_EQ(offset, by_all_marks);
                    else
                        EXPECT_GE(offset, by_all_marks);
                    if (right_mark > 0 && right_mark <= num_granules)
                        EXPECT_GE(offset, file.getNeededEnd(right_mark - 1, position));
                }
            }
        }
    }
}

/// All columns of a granule share one block (`compress_per_column_in_compact_parts = 0`),
/// so the marks of all columns but the first point inside it.
TEST(CompactPartRightOffset, ColumnsShareBlockOfGranule)
{
    MarksInCompressedFile::PlainArray plain;
    for (size_t block : {0UL, 100UL, 200UL})
        for (size_t offset : {0UL, 80UL, 90UL, 170UL})
            plain.push_back(MarkInCompressedFile{.offset_in_compressed_file = block, .offset_in_decompressed_block = offset});
    for (size_t i = 0; i < 4; ++i)
        plain.push_back(MarkInCompressedFile{.offset_in_compressed_file = 300, .offset_in_decompressed_block = 0});

    MergeTreeMarksGetter marks(MarksInCompressedFile::create(plain), 4);
    for (size_t position = 0; position < 4; ++position)
    {
        EXPECT_EQ(MergeTreeReaderStreamMultipleColumns::getRightOffsetByMarks(marks, 4, 300, 1, position, position), 100);
        EXPECT_EQ(MergeTreeReaderStreamMultipleColumns::getRightOffsetByMarks(marks, 4, 300, 2, position, position), 200);
        EXPECT_EQ(MergeTreeReaderStreamMultipleColumns::getRightOffsetByMarks(marks, 4, 300, 3, position, position), 300);
    }
}

/// A stripe of two granules of `a Array(UInt64) CODEC(...)`, whose sizes and elements are in separate blocks,
/// and then a 4 MiB String column. The elements of both granules are empty, so their marks equal the mark of
/// the data that follows them.
TEST(CompactPartRightOffset, EmptyLastSubstreamOfColumn)
{
    const std::vector<std::vector<MarkInCompressedFile>> rows = {
        {{0, 0}, {100, 0}, {200, 0}},
        {{100, 0}, {200, 0}, {4194504, 0}},
        {{8388808, 0}, {8388808, 0}, {8388808, 0}},
    };

    MarksInCompressedFile::PlainArray plain;
    for (const auto & row : rows)
        for (const auto & mark : row)
            plain.push_back(mark);

    MergeTreeMarksGetter marks(MarksInCompressedFile::create(plain), 3);
    auto right_offset = [&](size_t right_mark, size_t position, size_t first_position)
    { return MergeTreeReaderStreamMultipleColumns::getRightOffsetByMarks(marks, 3, 8388808, right_mark, position, first_position); };

    /// Inside the stripe the next granule of the column gives the exact end of the empty elements.
    EXPECT_EQ(right_offset(1, 1, 0), 100);
    /// At the end of the stripe the next data is the String column at the start of the stripe, which the marks
    /// cannot tell from data written before the column. The bound stays safe but reads the String column too.
    EXPECT_GE(right_offset(2, 1, 0), 200);

    /// The sizes are never empty, so their bound is the start of the elements.
    EXPECT_EQ(right_offset(1, 0, 0), 100);
    EXPECT_EQ(right_offset(2, 0, 0), 200);
}
