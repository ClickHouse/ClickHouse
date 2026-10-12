#include <gtest/gtest.h>

#include <Columns/ColumnString.h>

#include <Common/Exception.h>
#include <Common/randomSeed.h>
#include <Common/thread_local_rng.h>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
}

using namespace DB;

static pcg64 rng(randomSeed());

constexpr size_t bytes_per_string = sizeof(uint64_t);
/// Column should have enough bytes to be compressed
constexpr size_t column_size = ColumnString::min_size_to_compress / bytes_per_string + 42;

TEST(ColumnString, Incompressible)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        const uint64_t value = rng();
        memcpy(&chars[i * bytes_per_string], &value, sizeof(uint64_t));
        offsets.push_back((i + 1) * bytes_per_string);
    }

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    // When column is incompressible, we return the original column wrapped in CompressedColumn
    ASSERT_EQ(decompressed.get(), col.get());
    ASSERT_EQ(compressed->size(), col->size());
    ASSERT_EQ(compressed->allocatedBytes(), col->allocatedBytes());
    ASSERT_EQ(decompressed->size(), col->size());
    ASSERT_EQ(decompressed->allocatedBytes(), col->allocatedBytes());
}

TEST(ColumnString, CompressibleCharsAndIncompressibleOffsets)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        static const uint64_t value = 42;
        memcpy(&chars[i * bytes_per_string], &value, sizeof(uint64_t));
    }
    offsets.push_back(chars.size());

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    // For actually compressed column only compressed `chars` and `offsets` arrays are stored.
    // Upon decompression, a new column is created.
    ASSERT_NE(decompressed.get(), col.get());
    ASSERT_EQ(compressed->size(), col->size());
    ASSERT_LE(compressed->allocatedBytes(), col->allocatedBytes());
    ASSERT_EQ(decompressed->size(), col->size());
    ASSERT_LE(decompressed->allocatedBytes(), col->allocatedBytes());
}

TEST(ColumnString, CompressibleCharsAndCompressibleOffsets)
{
    auto col = ColumnString::create();
    auto & chars = col->getChars();
    auto & offsets = col->getOffsets();
    chars.resize(column_size * bytes_per_string);
    for (size_t i = 0; i < column_size; ++i)
    {
        static const uint64_t value = 42;
        memcpy(&chars[i * bytes_per_string], &value, sizeof(uint64_t));
        offsets.push_back((i + 1) * bytes_per_string);
    }

    auto compressed = col->compress(true);
    auto decompressed = compressed->decompress();
    // For actually compressed column only compressed `chars` and `offsets` arrays are stored.
    // Upon decompression, a new column is created.
    ASSERT_NE(decompressed.get(), col.get());
    ASSERT_EQ(compressed->size(), col->size());
    ASSERT_LE(compressed->allocatedBytes(), col->allocatedBytes());
    ASSERT_EQ(decompressed->size(), col->size());
    ASSERT_LE(decompressed->allocatedBytes(), col->allocatedBytes());
}

/// Build a ColumnString whose offsets claim more bytes than its `chars` array actually holds.
/// This is the inconsistent state produced upstream (e.g. a nested String column inside
/// Object/Dynamic/Variant after a serialization desync) that the copy constructor rejects but
/// that previously slipped into insertRangeFrom unchecked.
static ColumnString::MutablePtr makeStringColumnWithOffsetsBeyondChars(size_t last_offset_overshoot)
{
    auto column = ColumnString::create();
    column->insertData("aaaa", 4);
    column->insertData("bbbb", 4);
    column->insertData("cccc", 4);

    /// At this point offsets.back() == chars.size(). Push the final offset past the end of `chars`
    /// without growing `chars`, so the last string claims `last_offset_overshoot` extra bytes.
    auto & offsets = column->getOffsets();
    offsets.back() += last_offset_overshoot;

    return column;
}

/// Reproduces the heap-buffer-overflow in ColumnString::insertRangeFrom: when the source column's
/// offsets are inconsistent with its `chars` array, the internal memcpy reads past the end of
/// `chars`. The crash observed in CI / production has the stack
///   ColumnString::doInsertRangeFrom <- ColumnVariant::insertRangeFromImpl
///     <- ColumnDynamic::doInsertRangeFrom <- ColumnObject::doInsertRangeFrom <- merge
/// where the innermost over-read is exactly this memcpy.
///
/// Without the consistency guard this test triggers an ASan heap-buffer-overflow (it must abort on
/// the unfixed code). With the guard it throws INCORRECT_DATA instead of reading out of bounds.
TEST(ColumnString, InsertRangeFromInconsistentOffsetsThrows)
{
    auto source = makeStringColumnWithOffsetsBeyondChars(/*last_offset_overshoot=*/8);
    auto destination = ColumnString::create();

    try
    {
        destination->insertRangeFrom(*source, 0, source->size());
        FAIL() << "insertRangeFrom did not detect offsets inconsistent with chars";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
        ASSERT_NE(std::string::npos, std::string(e.message()).find("inconsistent with chars"));
    }
}

/// Same defect with a large overshoot, matching the magnitude seen in the symbolized CI crash
/// (a nested String over-read of hundreds of KB past a ~128 KB chars buffer).
TEST(ColumnString, InsertRangeFromInconsistentOffsetsLargeOvershoot)
{
    auto source = makeStringColumnWithOffsetsBeyondChars(/*last_offset_overshoot=*/256 * 1024);
    auto destination = ColumnString::create();

    EXPECT_THROW(destination->insertRangeFrom(*source, 0, source->size()), DB::Exception);
}

/// Build a ColumnString whose offsets are non-monotonic: a later offset is smaller than an earlier
/// one. `chars` is left untouched (large enough that the naive wrapped-sum check would pass), so the
/// only inconsistency is the decreasing offset itself.
static ColumnString::MutablePtr makeStringColumnWithDecreasingOffsets()
{
    auto column = ColumnString::create();
    column->insertData("aaaa", 4);
    column->insertData("bbbb", 4);
    column->insertData("cccc", 4);

    /// offsets == [4, 8, 12], chars.size() == 12. Drop the last offset below the second string's start,
    /// so offsetAt(3) == 2 < offsetAt(1) == 4. chars stays at 12 bytes, well above the wrapped sum (2).
    auto & offsets = column->getOffsets();
    offsets.back() = 2;

    return column;
}

/// Decreasing (non-monotonic) source offsets make `nested_length` underflow inside insertRangeFrom.
/// The naive guard `nested_offset + nested_length > chars.size()` wraps the sum back to the smaller end
/// offset and silently passes, letting the huge wrapped length reach resize()/memcpy(). Validating the
/// end offset before the subtraction catches it. Range [1, 3) has offsetAt(3) == 2 < offsetAt(1) == 4.
TEST(ColumnString, InsertRangeFromDecreasingOffsetsThrows)
{
    auto source = makeStringColumnWithDecreasingOffsets();
    auto destination = ColumnString::create();

    try
    {
        destination->insertRangeFrom(*source, 1, 2);
        FAIL() << "insertRangeFrom did not detect decreasing source offsets";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
        ASSERT_NE(std::string::npos, std::string(e.message()).find("inconsistent with chars"));
    }
}

/// Build a ColumnString whose offsets dip in the middle: an intermediate offset is smaller than the
/// one before it, while the final offset still matches `chars.size()`. Unlike the decreasing-endpoint
/// case, here offsetAt(start + length) stays a valid, in-bounds end offset, so the endpoint guard alone
/// would not notice the corruption.
static ColumnString::MutablePtr makeStringColumnWithDippingOffset()
{
    auto column = ColumnString::create();
    column->insertData("aaaa", 4);
    column->insertData("bbbb", 4);
    column->insertData("cccc", 4);

    /// offsets == [4, 8, 12], chars.size() == 12. Drop the middle offset below the first one, giving
    /// offsets [4, 2, 12]: offsetAt(1) == 4, offsetAt(3) == 12 (both within chars), but the copied
    /// offset offsets[1] == 2 < 4 dips in between.
    auto & offsets = column->getOffsets();
    offsets[1] = 2;

    return column;
}

/// An intermediate copied offset decreases (offsets[1] == 2 < nested_offset == 4) while the end offset
/// offsetAt(start + length) == 12 is still within chars.size() == 12, so the endpoint guard passes.
/// The rewrite loop would then compute offsets[1] - nested_offset == 2 - 4 and underflow, storing a
/// huge destination offset and leaving the result internally inconsistent. Validating every copied
/// offset for monotonicity catches it. Range [1, 3) copies offsets[1] == 2 then offsets[2] == 12.
TEST(ColumnString, InsertRangeFromDippingIntermediateOffsetThrows)
{
    auto source = makeStringColumnWithDippingOffset();
    auto destination = ColumnString::create();

    try
    {
        destination->insertRangeFrom(*source, 1, 2);
        FAIL() << "insertRangeFrom did not detect a dipping intermediate source offset";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
        ASSERT_NE(std::string::npos, std::string(e.message()).find("inconsistent with chars"));
    }
}

/// A consistent column of `num_rows` four-byte strings. 128 rows with an all-ones filter is two full
/// 64-row chunks and no tail, so the tests below drive the SIMD chunk branch of `filterArraysImpl`,
/// which sizes its copy from `offsets` alone (`chunk_size = offsets_pos[63] - offsets_pos[-1]`).
static ColumnString::MutablePtr makeFilterableStringColumn(size_t num_rows)
{
    auto column = ColumnString::create();
    for (size_t i = 0; i < num_rows; ++i)
        column->insertData("abcd", 4);

    return column;
}

/// A final offset past the end of `chars` makes that chunk copy run off the end of the source buffer.
/// The overshoot is far larger than `PaddedPODArray`'s padding, so without the guard the copy leaves
/// the allocation instead of landing in padding (ASan: `heap-buffer-overflow`, read of 262400 bytes).
TEST(ColumnString, FilterInconsistentOffsetsThrows)
{
    auto source = makeFilterableStringColumn(128);
    source->getOffsets().back() += 256 * 1024;
    IColumn::Filter filt(source->size(), 1);

    try
    {
        source->filter(filt, /*result_size_hint=*/ -1);
        FAIL() << "filter did not detect offsets inconsistent with chars";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
        ASSERT_NE(std::string::npos, std::string(e.message()).find("inconsistent with chars"));
    }
}

/// The mirror case: a final offset short of chars.size() is equally inconsistent, and the last chunk
/// then reports fewer bytes than the rows it covers.
TEST(ColumnString, FilterFinalOffsetBelowCharsSizeThrows)
{
    auto source = makeFilterableStringColumn(128);
    source->getOffsets().back() -= 4;
    IColumn::Filter filt(source->size(), 1);

    try
    {
        source->filter(filt, /*result_size_hint=*/ -1);
        FAIL() << "filter did not detect a final offset below chars.size()";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
        ASSERT_NE(std::string::npos, std::string(e.message()).find("inconsistent with chars"));
    }
}

/// The in-place overload sizes its moves from the same offsets (`filterArraysImplInPlace`), so it
/// needs its own guard.
TEST(ColumnString, FilterInPlaceInconsistentOffsetsThrows)
{
    auto source = makeFilterableStringColumn(128);
    source->getOffsets().back() += 256 * 1024;
    IColumn::Filter filt(source->size(), 1);

    try
    {
        source->filter(filt);
        FAIL() << "in-place filter did not detect offsets inconsistent with chars";
    }
    catch (const DB::Exception & e)
    {
        ASSERT_EQ(e.code(), DB::ErrorCodes::INCORRECT_DATA);
        ASSERT_NE(std::string::npos, std::string(e.message()).find("inconsistent with chars"));
    }
}

/// A consistent column must still filter correctly and not trip the new guard. The mask passes rows
/// 0-63 as a block (the all-pass chunk branch), then every third row (the per-row branch inside a
/// chunk, including the offset adjustment a non-first chunk needs), and leaves a partial trailing
/// chunk for the scalar tail loop.
TEST(ColumnString, FilterConsistentColumnStillWorks)
{
    auto column = makeFilterableStringColumn(200);

    IColumn::Filter filt(column->size(), 0);
    size_t expected = 0;
    for (size_t i = 0; i < filt.size(); ++i)
        if (i < 64 || i % 3 == 0)
        {
            filt[i] = 1;
            ++expected;
        }

    auto filtered = column->filter(filt, /*result_size_hint=*/ -1);
    ASSERT_EQ(filtered->size(), expected);
    const auto & filtered_str = assert_cast<const ColumnString &>(*filtered);
    ASSERT_EQ(filtered_str.getChars().size(), expected * 4); /// `ColumnString` values are not zero-terminated
    for (size_t i = 0; i < filtered_str.size(); ++i)
        ASSERT_EQ(filtered_str.getDataAt(i), std::string_view("abcd"));
}
