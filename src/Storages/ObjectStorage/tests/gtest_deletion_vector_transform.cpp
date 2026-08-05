#include <Storages/ObjectStorage/DataLakes/DeletionVectorTransform.h>
#include <Storages/ObjectStorage/DataLakes/DeletionVectorBitmap.h>
#include <Columns/ColumnsNumber.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/IInputFormat.h>

#include <gtest/gtest.h>

#include <random>
#include <vector>


namespace
{

using Bitmap = DB::DeletionVectorBitmap;

/// Deletion vectors carry absolute row numbers, so nothing in the transform may assume the chunk
/// starts at zero.
constexpr UInt64 row_offset = 1'000'000'000;

struct Input
{
    /// Value stored in the chunk's single column, one per row that reached the chunk.
    std::vector<UInt64> values;
    std::optional<DB::IColumnFilter> applied_filter;
};

/// Builds a chunk holding the row numbers `range_begin .. range_begin + num_row_numbers`, minus the
/// ones `keep` rejects. `keep` stands for a filter some upstream step has already applied.
template <typename Keep>
Input makeInput(size_t num_row_numbers, bool prefiltered, Keep && keep)
{
    Input input;
    if (prefiltered)
        input.applied_filter.emplace(num_row_numbers, 1);

    for (size_t i = 0; i < num_row_numbers; ++i)
    {
        if (prefiltered && !keep(i))
        {
            (*input.applied_filter)[i] = 0;
            continue;
        }
        input.values.push_back(row_offset + i);
    }
    return input;
}

/// `IColumnFilter` is a `PODArray`, which has no copy constructor.
std::optional<DB::IColumnFilter> copyFilter(const std::optional<DB::IColumnFilter> & filter)
{
    if (!filter)
        return {};
    return std::optional<DB::IColumnFilter>(std::in_place, filter->begin(), filter->end());
}

DB::Chunk makeChunk(const Input & input)
{
    auto column = DB::ColumnUInt64::create();
    for (UInt64 value : input.values)
        column->insertValue(value);

    DB::Columns columns;
    columns.emplace_back(std::move(column));

    DB::Chunk chunk(std::move(columns), input.values.size());
    chunk.getChunkInfos().add(std::make_shared<DB::ChunkInfoRowNumbers>(row_offset, copyFilter(input.applied_filter)));
    return chunk;
}

/// What the transform is expected to produce: the surviving row numbers, and the upstream filter
/// with the deleted rows cleared out of it.
struct Expected
{
    std::vector<UInt64> values;
    std::optional<DB::IColumnFilter> applied_filter;
};

Expected expectedResult(const Input & input, const Bitmap & excluded_rows)
{
    Expected expected;
    expected.applied_filter = copyFilter(input.applied_filter);

    for (UInt64 value : input.values)
    {
        if (excluded_rows.contains(value))
        {
            if (expected.applied_filter)
                (*expected.applied_filter)[value - row_offset] = 0;
        }
        else
        {
            expected.values.push_back(value);
        }
    }

    /// Without an upstream filter the transform produces one from scratch, indexed by position in
    /// the chunk rather than by row number.
    if (!input.applied_filter)
    {
        DB::IColumnFilter filter(input.values.size(), 1);
        for (size_t i = 0; i < input.values.size(); ++i)
            filter[i] = !excluded_rows.contains(input.values[i]);
        /// A chunk that loses nothing keeps its (absent) filter.
        if (expected.values.size() != input.values.size())
            expected.applied_filter = std::move(filter);
    }

    return expected;
}

void checkTransform(const Input & input, const Bitmap & excluded_rows, const String & what)
{
    const Expected expected = expectedResult(input, excluded_rows);

    DB::Chunk chunk = makeChunk(input);
    DB::DeletionVectorTransform::transform(chunk, excluded_rows);

    ASSERT_EQ(chunk.getNumRows(), expected.values.size()) << what;
    ASSERT_EQ(chunk.getNumColumns(), 1u) << what;

    const auto & column = assert_cast<const DB::ColumnUInt64 &>(*chunk.getColumns()[0]);
    ASSERT_EQ(column.size(), expected.values.size()) << what;
    for (size_t i = 0; i < expected.values.size(); ++i)
        ASSERT_EQ(column.getElement(i), expected.values[i]) << what << ", row " << i;

    /// Downstream steps recover `_row_number` from this mask, so a wrong index remapping has to be
    /// caught here even when the surviving rows happen to come out right.
    const auto chunk_info = chunk.getChunkInfos().get<DB::ChunkInfoRowNumbers>();
    ASSERT_TRUE(chunk_info) << what;
    ASSERT_EQ(chunk_info->row_num_offset, row_offset) << what;
    ASSERT_EQ(chunk_info->applied_filter.has_value(), expected.applied_filter.has_value()) << what;
    if (expected.applied_filter)
    {
        ASSERT_EQ(chunk_info->applied_filter->size(), expected.applied_filter->size()) << what;
        for (size_t i = 0; i < expected.applied_filter->size(); ++i)
            ASSERT_EQ((*chunk_info->applied_filter)[i], (*expected.applied_filter)[i]) << what << ", index " << i;
    }
}

}

/// The deleted row numbers are enumerated from the bitmap, so the position of each one inside the
/// chunk has to be recovered from the upstream filter. That remapping runs over spans of varying
/// length and switches strategy on span length, so the strides below straddle the switch.
TEST(DeletionVectorTransform, DeletionStrides)
{
    constexpr size_t num_row_numbers = 4096;

    for (bool prefiltered : {false, true})
    {
        for (size_t deletion_stride : {1, 2, 3, 4, 7, 63, 64, 65, 127, 128, 1000, 4095})
        {
            for (size_t prefilter_stride : {2, 3, 5, 64, 65})
            {
                if (!prefiltered && prefilter_stride != 2)
                    continue;

                const Input input = makeInput(num_row_numbers, prefiltered,
                    [&](size_t i) { return i % prefilter_stride != 0; });

                Bitmap excluded_rows;
                for (size_t i = 0; i < num_row_numbers; i += deletion_stride)
                    excluded_rows.add(row_offset + i);

                checkTransform(input, excluded_rows,
                    fmt::format("prefiltered = {}, deletion stride = {}, prefilter stride = {}",
                        prefiltered, deletion_stride, prefilter_stride));
            }
        }
    }
}

TEST(DeletionVectorTransform, NonBinaryAppliedFilter)
{
    Input input = makeInput(8, true, [](size_t i) { return i != 2 && i != 6; });
    ASSERT_TRUE(input.applied_filter);

    /// Filters treat every non-zero byte as true, so index remapping must count bytes rather than
    /// summing their values.
    for (size_t i = 0; i < input.applied_filter->size(); ++i)
    {
        if ((*input.applied_filter)[i])
            (*input.applied_filter)[i] = static_cast<UInt8>(i + 2);
    }

    Bitmap excluded_rows;
    excluded_rows.add(row_offset + 4);
    checkTransform(input, excluded_rows, "non-binary applied filter");
}

/// Row numbers on both sides of the chunk must be ignored, and the scan must start at the chunk.
TEST(DeletionVectorTransform, RowNumbersOutsideTheChunk)
{
    constexpr size_t num_row_numbers = 512;

    for (bool prefiltered : {false, true})
    {
        const Input input = makeInput(num_row_numbers, prefiltered, [](size_t i) { return i % 3 != 0; });

        Bitmap excluded_rows;
        for (size_t i = 1; i <= 1000; ++i)
        {
            excluded_rows.add(row_offset - i);
            excluded_rows.add(row_offset + num_row_numbers + i);
        }
        excluded_rows.add(row_offset);
        excluded_rows.add(row_offset + num_row_numbers - 1);

        checkTransform(input, excluded_rows, fmt::format("prefiltered = {}", prefiltered));
    }
}

TEST(DeletionVectorTransform, NothingToDelete)
{
    for (bool prefiltered : {false, true})
    {
        const Input input = makeInput(1024, prefiltered, [](size_t i) { return i % 3 != 0; });

        Bitmap empty;
        checkTransform(input, empty, fmt::format("empty bitmap, prefiltered = {}", prefiltered));

        Bitmap elsewhere;
        for (size_t i = 1; i <= 100; ++i)
            elsewhere.add(row_offset + 1024 + i);
        checkTransform(input, elsewhere, fmt::format("bitmap past the chunk, prefiltered = {}", prefiltered));
    }
}

TEST(DeletionVectorTransform, EmptyChunk)
{
    Bitmap excluded_rows;
    excluded_rows.add(row_offset);

    for (bool prefiltered : {false, true})
    {
        const Input input = makeInput(0, prefiltered, [](size_t) { return true; });
        checkTransform(input, excluded_rows, fmt::format("prefiltered = {}", prefiltered));
    }
}

TEST(DeletionVectorTransform, EverythingIsDeleted)
{
    for (bool prefiltered : {false, true})
    {
        const Input input = makeInput(1024, prefiltered, [](size_t i) { return i % 3 != 0; });

        Bitmap excluded_rows;
        for (size_t i = 0; i < 1024; ++i)
            excluded_rows.add(row_offset + i);

        checkTransform(input, excluded_rows, fmt::format("prefiltered = {}", prefiltered));
    }
}

/// Irregular gaps mix the short and long spans that regular strides keep apart.
TEST(DeletionVectorTransform, RandomizedAgainstPerRowLookup)
{
    std::mt19937_64 random(20250805);

    for (size_t iteration = 0; iteration < 300; ++iteration)
    {
        const size_t num_row_numbers = std::uniform_int_distribution<size_t>(1, 3000)(random);
        const bool prefiltered = std::uniform_int_distribution<int>(0, 1)(random) != 0;

        /// Spread the two probabilities over their whole range so that sparse, dense and
        /// everything-in-between all show up.
        const double keep_probability = std::uniform_real_distribution<double>(0.0, 1.0)(random);
        const double delete_probability = std::uniform_real_distribution<double>(0.0, 1.0)(random);

        std::bernoulli_distribution keep(keep_probability);
        const Input input = makeInput(num_row_numbers, prefiltered, [&](size_t) { return keep(random); });

        Bitmap excluded_rows;
        std::bernoulli_distribution del(delete_probability);
        for (size_t i = 0; i < num_row_numbers; ++i)
        {
            if (del(random))
                excluded_rows.add(row_offset + i);
        }

        checkTransform(input, excluded_rows,
            fmt::format("iteration {}: rows = {}, prefiltered = {}, keep = {:.3f}, delete = {:.3f}",
                iteration, num_row_numbers, prefiltered, keep_probability, delete_probability));
    }
}
