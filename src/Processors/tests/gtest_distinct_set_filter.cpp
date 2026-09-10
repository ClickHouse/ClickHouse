#include <gtest/gtest.h>

#include <optional>
#include <set>
#include <thread>

#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Transforms/DistinctSetFilter.h>
#include <Common/assert_cast.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/iota.h>

namespace DB::ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

using namespace DB;

namespace
{

using RowsMultiset = std::multiset<std::vector<Field>>;

RowsMultiset collectRows(const Columns & columns, size_t num_rows)
{
    RowsMultiset rows;
    for (size_t i = 0; i < num_rows; ++i)
    {
        std::vector<Field> row;
        row.reserve(columns.size());
        for (const auto & column : columns)
            row.push_back((*column)[i]);
        rows.insert(std::move(row));
    }
    return rows;
}

/// Feeds the chunks through the filter and checks that the keys extracted from the set are exactly
/// the emitted (distinct) rows.
void checkExtractionRoundTrip(
    const Block & header, std::vector<Columns> chunks, size_t max_batch_rows = 1, bool require_extractable_keys = false)
{
    std::optional<DistinctSetFilter> filter(
        std::in_place, header, Names{}, SizeLimits{}, /*skip_null_keys_=*/ false, require_extractable_keys);

    RowsMultiset emitted;
    size_t emitted_count = 0;
    for (auto & columns : chunks)
    {
        const size_t num_rows = columns.front()->size();
        Chunk filtered = filter->filter(Chunk(std::move(columns), num_rows));
        if (filtered.hasRows())
        {
            emitted_count += filtered.getNumRows();
            auto rows = collectRows(filtered.getColumns(), filtered.getNumRows());
            emitted.merge(rows);
        }
    }

    const size_t expected_count = filter->getTotalRowCount();
    auto extractor = std::move(*filter).extractKeys();
    filter.reset();

    std::vector<Columns> batches;
    size_t extracted_count = 0;
    while (true)
    {
        auto batch = extractor->next(max_batch_rows, /*max_bytes=*/ 0);
        if (batch.empty())
            break;

        const size_t num_rows = batch.front()->size();
        EXPECT_GT(num_rows, 0);
        EXPECT_LE(num_rows, max_batch_rows);
        extracted_count += num_rows;

        Columns columns;
        for (auto & column : batch)
            columns.push_back(std::move(column));
        batches.push_back(std::move(columns));
    }

    EXPECT_TRUE(extractor->next(max_batch_rows, /*max_bytes=*/ 0).empty());
    extractor.reset();

    /// The materialized values remain readable after the extractor and its table are released.
    RowsMultiset extracted;
    for (const auto & columns : batches)
    {
        auto rows = collectRows(columns, columns.front()->size());
        extracted.merge(rows);
    }

    EXPECT_EQ(extracted_count, emitted_count);
    EXPECT_EQ(extracted_count, expected_count);
    EXPECT_EQ(extracted, emitted);
}

template <typename ColumnType, typename T>
ColumnPtr makeNumberColumn(const std::vector<T> & values)
{
    auto column = ColumnType::create();
    for (const auto & value : values)
        column->insertValue(value);
    return column;
}

ColumnPtr makeColumn(const std::vector<UInt64> & values)
{
    auto column = ColumnUInt64::create();
    for (const auto value : values)
        column->insertValue(value);
    return column;
}

ColumnPtr makeStringColumn(const std::vector<String> & values)
{
    auto column = ColumnString::create();
    for (const auto & value : values)
        column->insertData(value.data(), value.size());
    return column;
}

}

TEST(DistinctSetFilterExtraction, Key8)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "k")};
    checkExtractionRoundTrip(
        header,
        {{makeNumberColumn<ColumnUInt8, UInt8>({0, 1, 1, 255})}, {makeNumberColumn<ColumnUInt8, UInt8>({255, 7})}});
}

TEST(DistinctSetFilterExtraction, Key16)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt16>(), "k")};
    checkExtractionRoundTrip(header, {{makeNumberColumn<ColumnUInt16, UInt16>({0, 1, 40000, 40000, 65535})}});
}

TEST(DistinctSetFilterExtraction, Key32)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt32>(), "k")};
    checkExtractionRoundTrip(header, {{makeNumberColumn<ColumnUInt32, UInt32>({0, 1, 2, 1000000000, 0})}});
}

TEST(DistinctSetFilterExtraction, Key64)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")};
    checkExtractionRoundTrip(header, {{makeNumberColumn<ColumnUInt64, UInt64>({0, 1, 2, 1, 0, 999999999999})}});
}

TEST(DistinctSetFilterExtraction, Key64Batching)
{
    std::vector<UInt64> values(100);
    for (size_t i = 0; i < values.size(); ++i)
        values[i] = i % 37;

    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")};
    checkExtractionRoundTrip(header, {{makeNumberColumn<ColumnUInt64, UInt64>(values)}}, /*max_batch_rows=*/ 10);
}

TEST(DistinctSetFilterExtraction, KeyString)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "k")};
    checkExtractionRoundTrip(
        header,
        {{makeStringColumn({"", "a", "a", "some long string that certainly does not fit inline"})},
         {makeStringColumn({"", "b", "some long string that certainly does not fit inline"})}});
}

TEST(DistinctSetFilterExtraction, KeyFixedString)
{
    auto column = ColumnFixedString::create(40);
    for (const auto & value : {String(40, 'a'), String(40, 'b'), String(40, 'a'), String(40, 'c')})
        column->insertData(value.data(), value.size());

    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeFixedString>(40), "k")};
    checkExtractionRoundTrip(header, {{std::move(column)}});
}

TEST(DistinctSetFilterExtraction, KeysFixedMultipleColumns)
{
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt32>(), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "b")};
    checkExtractionRoundTrip(
        header,
        {{makeNumberColumn<ColumnUInt32, UInt32>({1, 1, 2, 2}), makeNumberColumn<ColumnUInt64, UInt64>({10, 10, 10, 20})},
         {makeNumberColumn<ColumnUInt32, UInt32>({1, 3}), makeNumberColumn<ColumnUInt64, UInt64>({10, 30})}});
}

TEST(DistinctSetFilterExtraction, KeysFixedReorderedBySize)
{
    /// The prepared-keys packing lays the columns out by size in descending order, not in the column
    /// order; the extraction must invert that permutation.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt32>(), "b"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt16>(), "c")};
    checkExtractionRoundTrip(
        header,
        {{makeNumberColumn<ColumnUInt8, UInt8>({1, 1, 2}),
          makeNumberColumn<ColumnUInt32, UInt32>({100, 100, 200}),
          makeNumberColumn<ColumnUInt16, UInt16>({7, 7, 8})}});
}

TEST(DistinctSetFilterExtraction, NullableKey)
{
    auto make_column = [](const std::vector<std::optional<UInt64>> & values)
    {
        auto column = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
        for (const auto & value : values)
        {
            if (value)
                column->insert(Field(*value));
            else
                column->insertDefault();
        }
        return ColumnPtr(std::move(column));
    };

    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "k")};
    checkExtractionRoundTrip(header, {{make_column({1, {}, 2, {}, 1, 42})}});
}

TEST(DistinctSetFilterExtraction, FloatBitPatternsSurviveExtraction)
{
    /// 0. and -0. are different distinct values (binary comparison); the extraction must preserve both.
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeFloat64>(), "k")};

    DistinctSetFilter filter(header, {}, SizeLimits{});
    auto column = makeNumberColumn<ColumnFloat64, Float64>({0., -0., 0., -0.});
    Chunk filtered = filter.filter(Chunk({column}, 4));
    ASSERT_EQ(filtered.getNumRows(), 2u);

    auto extractor = std::move(filter).extractKeys();
    auto batch = extractor->next(2, /*max_bytes=*/ 0);
    ASSERT_EQ(batch.size(), 1u);
    EXPECT_TRUE(extractor->next(2, /*max_bytes=*/ 0).empty());
    const auto & extracted = assert_cast<const ColumnFloat64 &>(*batch[0]).getData();
    ASSERT_EQ(extracted.size(), 2u);
    EXPECT_NE(std::signbit(extracted[0]), std::signbit(extracted[1]));
}

TEST(DistinctSetFilterSemantics, ThrowModeAllowsReachingTheLimitExactly)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")};

    SizeLimits limits(/*max_rows=*/ 2, /*max_bytes=*/ 0, OverflowMode::THROW);
    DistinctSetFilter filter(header, {}, limits);

    /// The 'throw' mode uses a strict comparison: a set of exactly `max_rows` keys is allowed.
    EXPECT_EQ(filter.filter(Chunk({makeColumn({1, 2})}, 2)).getNumRows(), 2u);
    EXPECT_ANY_THROW(filter.filter(Chunk({makeColumn({3})}, 1)));
}

TEST(DistinctSetFilterSemantics, BreakModeKeepsTheCrossingChunkAndReportsTheLimit)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")};

    SizeLimits limits(/*max_rows=*/ 2, /*max_bytes=*/ 0, OverflowMode::BREAK);
    DistinctSetFilter filter(header, {}, limits);

    EXPECT_EQ(filter.filter(Chunk({makeColumn({1})}, 1)).getNumRows(), 1u);
    EXPECT_FALSE(filter.isLimitReached());

    /// The 'break' mode returns the new rows of the chunk that crosses the limit (their keys are in
    /// the set) and reports the limit, so that the caller stops reading with a partial result instead
    /// of discarding the crossing chunk.
    EXPECT_EQ(filter.filter(Chunk({makeColumn({2, 3})}, 2)).getNumRows(), 2u);
    EXPECT_EQ(filter.getTotalRowCount(), 3u);
    EXPECT_TRUE(filter.isLimitReached());
}

TEST(DistinctSetFilterSemantics, LowCardinalityPathMatchesGenericPath)
{
    /// The `LowCardinality` fast path must produce exactly the same distinct rows as the generic hash path
    /// over the equal data.
    auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const Block lc_header = {ColumnWithTypeAndName(lc_type, "k")};
    const Block str_header = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "k")};

    const std::vector<std::vector<String>> chunks_data
        = {{"a", "b", "a"}, {"b", "c", "c"}, {"a", "d"}, {"d", "d", "d"}};

    DistinctSetFilter lc_filter(lc_header, {}, SizeLimits{});
    DistinctSetFilter str_filter(str_header, {}, SizeLimits{});

    std::multiset<String> lc_emitted;
    std::multiset<String> str_emitted;

    for (const auto & values : chunks_data)
    {
        auto lc_column = lc_type->createColumn();
        for (const auto & value : values)
            lc_column->insertData(value.data(), value.size());

        auto lc_result = lc_filter.filter(Chunk(Columns{std::move(lc_column)}, values.size()));
        for (size_t i = 0; i < lc_result.getNumRows(); ++i)
            lc_emitted.insert((*lc_result.getColumns()[0])[i].safeGet<String>());

        auto str_result = str_filter.filter(Chunk(Columns{makeStringColumn(values)}, values.size()));
        for (size_t i = 0; i < str_result.getNumRows(); ++i)
            str_emitted.insert((*str_result.getColumns()[0])[i].safeGet<String>());
    }

    EXPECT_EQ(lc_emitted, str_emitted);
    EXPECT_EQ(lc_emitted, (std::multiset<String>{"a", "b", "c", "d"}));
}

TEST(DistinctSetFilterExtraction, Keys32TwoSmallColumns)
{
    /// `UInt8` and `UInt16` use `keys32`, with the wider column packed first.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt16>(), "b")};

    checkExtractionRoundTrip(
        header,
        {{makeNumberColumn<ColumnUInt8, UInt8>({1, 2, 1, 255}), makeNumberColumn<ColumnUInt16, UInt16>({7, 7, 40000, 40000})}});
}

TEST(DistinctSetFilterExtraction, Keys256MultipleColumns)
{
    /// Three `UInt64` columns occupy 24 bytes and use `keys256`.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "b"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "c")};

    checkExtractionRoundTrip(
        header,
        {{makeNumberColumn<ColumnUInt64, UInt64>({1, 1, 2, 1}),
          makeNumberColumn<ColumnUInt64, UInt64>({10, 10, 10, 11}),
          makeNumberColumn<ColumnUInt64, UInt64>({100, 100, 100, 100})}});
}

TEST(DistinctSetFilterExtraction, NullableKeys256TwoColumns)
{
    auto make_column = [](const std::vector<std::optional<UInt64>> & values)
    {
        auto column = ColumnNullable::create(ColumnUInt64::create(), ColumnUInt8::create());
        for (const auto & value : values)
        {
            if (value)
                column->insert(Field(*value));
            else
                column->insertDefault();
        }
        return ColumnPtr(std::move(column));
    };

    /// Two nullable `UInt64` columns need 16 value bytes plus a null bitmap and use `nullable_keys256`.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "b")};

    checkExtractionRoundTrip(
        header, {{make_column({1, {}, 1, {}, 1}), make_column({2, 2, {}, {}, 2})}});
}

TEST(DistinctSetFilterExtraction, MixedNullableAndPlainColumns)
{
    auto make_nullable_column = [](const std::vector<std::optional<UInt64>> & values)
    {
        auto column = ColumnNullable::create(ColumnUInt32::create(), ColumnUInt8::create());
        for (const auto & value : values)
        {
            if (value)
                column->insert(Field(*value));
            else
                column->insertDefault();
        }
        return ColumnPtr(std::move(column));
    };

    /// A nullable and a plain column use `nullable_keys128`, exercising both nullability paths in unpacking.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt32>(), "b")};

    checkExtractionRoundTrip(
        header,
        {{make_nullable_column({1, {}, 1, {}}), makeNumberColumn<ColumnUInt32, UInt32>({5, 5, 6, 6})}});
}

namespace
{

/// A `LowCardinality(String)` column of `num_rows` rows whose dictionary holds at least `dictionary_size`
/// entries: the rows are cut from a column that used them all, and a cut keeps the dictionary.
ColumnPtr makeLowCardinalityColumnWithLargeDictionary(const DataTypePtr & lc_type, size_t dictionary_size, size_t num_rows)
{
    auto column = lc_type->createColumn();
    for (size_t i = 0; i < dictionary_size; ++i)
    {
        const auto value = std::to_string(i);
        column->insertData(value.data(), value.size());
    }

    auto rows = column->cut(0, num_rows);
    EXPECT_GE(assert_cast<const ColumnLowCardinality &>(*rows).getDictionary().size(), dictionary_size);
    return rows;
}

}

TEST(DistinctLowCardinalityFilter, ReleasesDisabledDictionaryState)
{
    MemoryTracker query{&total_memory_tracker, VariableContext::Process, false};
    std::thread([&]
    {
        ThreadStatus thread_status;
        thread_status.memory_tracker.setParent(&query);
        thread_status.untracked_memory_limit = 0;
        const auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
        const size_t dictionary_size = 200000;
        const Columns dictionaries{
            makeLowCardinalityColumnWithLargeDictionary(type, dictionary_size, 6),
            makeLowCardinalityColumnWithLargeDictionary(type, dictionary_size + 1, 6)};
        DistinctLowCardinalityFilter filter;
        for (size_t i = 0; i < 4; ++i)
        {
            const auto column = dictionaries[i % dictionaries.size()]->cut(i, 1);
            const auto mask = filter.buildMaskIfApplicable(*column, 1);
            ASSERT_TRUE(mask.has_value());
            ASSERT_EQ(mask->size(), 1);
            EXPECT_EQ((*mask)[0], 1);
        }

        const auto column = dictionaries.front()->cut(4, 1);
        const size_t bitmap_bytes = filter.getTotalByteCount();
        ASSERT_GE(bitmap_bytes, 2 * dictionary_size);
        const auto memory_before = query.get();
        const auto mask = filter.buildMaskIfApplicable(*column, 1);
        const auto memory_after = query.get();
        ASSERT_TRUE(mask.has_value());
        ASSERT_EQ(mask->size(), 1);
        EXPECT_EQ((*mask)[0], 1);
        EXPECT_EQ(filter.getTotalByteCount(), 0);
        EXPECT_LE(
            memory_after, memory_before - static_cast<Int64>(bitmap_bytes) + static_cast<Int64>(mask->allocated_bytes()));
        EXPECT_FALSE(filter.buildMaskIfApplicable(*dictionaries.front(), 6).has_value());
        EXPECT_EQ(filter.getTotalByteCount(), 0);
    }).join();
}

TEST(DistinctLowCardinalityFilter, RetainsUsefulDictionaryState)
{
    const auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const auto dictionary = makeLowCardinalityColumnWithLargeDictionary(type, 100, 2);
    const auto repeated = dictionary->cut(0, 1);
    DistinctLowCardinalityFilter filter;
    for (size_t i = 0; i < 6; ++i)
    {
        const auto mask = filter.buildMaskIfApplicable(*repeated, 1);
        ASSERT_TRUE(mask.has_value());
        EXPECT_EQ(mask->size(), i == 0 ? 1 : 0);
        EXPECT_GE(filter.getTotalByteCount(), 100);
    }
    const auto next = dictionary->cut(1, 1);
    const auto mask = filter.buildMaskIfApplicable(*next, 1);
    ASSERT_TRUE(mask.has_value());
    ASSERT_EQ(mask->size(), 1);
    EXPECT_EQ((*mask)[0], 1);
}

TEST(DistinctSetFilterSemantics, DisabledLowCardinalityStateDoesNotConsumeByteLimit)
{
    const auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const Block header = {ColumnWithTypeAndName(type, "k")};
    const size_t dictionary_size = 200000;
    const auto small_dictionary = makeLowCardinalityColumnWithLargeDictionary(type, 4, 4);
    const auto large_dictionary = makeLowCardinalityColumnWithLargeDictionary(type, dictionary_size, 6);
    for (const bool require_extractable_keys : {false, true})
    {
        SCOPED_TRACE(require_extractable_keys);
        for (const auto overflow_mode : {OverflowMode::THROW, OverflowMode::BREAK})
        {
            SCOPED_TRACE(static_cast<int>(overflow_mode));
            const SizeLimits limits(/*max_rows=*/ 0, /*max_bytes=*/ dictionary_size / 2, overflow_mode);
            DistinctSetFilter filter(header, {}, limits, /*skip_null_keys_=*/ false, require_extractable_keys);
            for (size_t i = 0; i < 4; ++i)
                ASSERT_EQ(filter.filter(Chunk({small_dictionary->cut(i, 1)}, 1)).getNumRows(), 1);

            /// The fifth chunk disables the bitmap while the hash set retains all previously seen keys.
            auto result = filter.filter(Chunk({large_dictionary->cut(3, 2)}, 2));
            ASSERT_EQ(result.getNumRows(), 1);
            EXPECT_EQ((*result.getColumns().front())[0].safeGet<String>(), "4");
            EXPECT_LT(filter.getTotalByteCount(), limits.max_bytes);
            EXPECT_FALSE(filter.isLimitReached());

            result = filter.filter(Chunk({large_dictionary}, 6));
            ASSERT_EQ(result.getNumRows(), 1);
            EXPECT_EQ((*result.getColumns().front())[0].safeGet<String>(), "5");
            EXPECT_EQ(filter.getTotalRowCount(), 6);
            EXPECT_LT(filter.getTotalByteCount(), limits.max_bytes);
            EXPECT_FALSE(filter.isLimitReached());
        }
    }
}

TEST(DistinctSetFilterSemantics, LowCardinalityBitmapsCountTowardsTheByteSize)
{
    /// The `LowCardinality` fast path keeps a bitmap of the seen indices per dictionary. Its size is that
    /// of the dictionary, not of the data seen, so a few rows over a large dictionary occupy far more than
    /// their keys in the set, and the reported size must include it.
    auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const Block header = {ColumnWithTypeAndName(lc_type, "k")};
    const size_t dictionary_size = 200000;

    DistinctSetFilter filter(header, {}, SizeLimits{});
    filter.filter(Chunk(Columns{makeLowCardinalityColumnWithLargeDictionary(lc_type, dictionary_size, 3)}, 3));
    EXPECT_GE(filter.getTotalByteCount(), dictionary_size);
}

TEST(DistinctSetFilterSemantics, ByteLimitSeesTheLowCardinalityBitmaps)
{
    /// The same three rows stay far below the limit as plain strings; over a large dictionary the bitmap
    /// of the fast path exceeds it, and the limit must notice.
    auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const Block lc_header = {ColumnWithTypeAndName(lc_type, "k")};
    const Block str_header = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "k")};
    const size_t dictionary_size = 200000;
    SizeLimits limits(/*max_rows=*/ 0, /*max_bytes=*/ dictionary_size / 2, OverflowMode::THROW);

    DistinctSetFilter str_filter(str_header, {}, limits);
    EXPECT_NO_THROW(str_filter.filter(Chunk(Columns{makeStringColumn({"0", "1", "2"})}, 3)));

    DistinctSetFilter lc_filter(lc_header, {}, limits);
    EXPECT_ANY_THROW(lc_filter.filter(Chunk(Columns{makeLowCardinalityColumnWithLargeDictionary(lc_type, dictionary_size, 3)}, 3)));
}

TEST(DistinctSetFilterSemantics, DuplicateKeysEnforceByteLimit)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const Block header = {ColumnWithTypeAndName(type, "k")};
    const size_t dictionary_size = 200000;

    for (const bool require_extractable_keys : {false, true})
    {
        SCOPED_TRACE(require_extractable_keys);
        for (const auto overflow_mode : {OverflowMode::THROW, OverflowMode::BREAK})
        {
            SCOPED_TRACE(static_cast<int>(overflow_mode));
            const SizeLimits limits(/*max_rows=*/ 0, /*max_bytes=*/ dictionary_size / 2, overflow_mode);
            DistinctSetFilter filter(header, {}, limits, /*skip_null_keys_=*/ false, require_extractable_keys);
            const auto small_dictionary = makeLowCardinalityColumnWithLargeDictionary(type, 3, 3);

            ASSERT_EQ(filter.filter(Chunk({small_dictionary}, 3)).getNumRows(), 3);
            auto larger_dictionary = makeLowCardinalityColumnWithLargeDictionary(type, 100, 3);
            EXPECT_FALSE(filter.filter(Chunk({larger_dictionary}, 3)).hasRows());
            EXPECT_FALSE(filter.isLimitReached());
            ASSERT_LT(filter.getTotalByteCount(), limits.max_bytes);

            /// A new dictionary retains a bitmap even when its rows contain only previously seen keys.
            auto duplicates = makeLowCardinalityColumnWithLargeDictionary(type, dictionary_size, 3);
            if (overflow_mode == OverflowMode::THROW)
            {
                try
                {
                    filter.filter(Chunk({duplicates}, 3));
                    ADD_FAILURE() << "Expected the dictionary bitmap to exceed the byte limit";
                }
                catch (const Exception & exception)
                {
                    EXPECT_EQ(exception.code(), ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
                }
            }
            else
            {
                EXPECT_FALSE(filter.filter(Chunk({duplicates}, 3)).hasRows());
                EXPECT_TRUE(filter.isLimitReached());
            }
            EXPECT_EQ(filter.getTotalRowCount(), 3);
            EXPECT_GT(filter.getTotalByteCount(), limits.max_bytes);
        }
    }
}

TEST(DistinctSetFilterExtraction, SerializedKeysOnRequest)
{
    /// Two `String` keys fall to the generic `hashed` method, which cannot materialize the keys back; a
    /// consumer that needs them gets the `serialized` method instead, which stores them.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "a"), ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "b")};
    checkExtractionRoundTrip(
        header,
        {{makeStringColumn({"a", "b", "a"}), makeStringColumn({"x", "y", "x"})}, {makeStringColumn({"a", "c"}), makeStringColumn({"y", "z"})}},
        /*max_batch_rows=*/ 2,
        /*require_extractable_keys=*/ true);
}

TEST(DistinctSetFilterExtraction, SerializedLowCardinalityKey)
{
    auto lc_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    const Block header = {ColumnWithTypeAndName(lc_type, "k")};

    std::vector<Columns> chunks;
    for (const auto & values : std::vector<std::vector<String>>{{"a", "b", "a"}, {"c", "a", "d"}})
    {
        auto column = lc_type->createColumn();
        for (const auto & value : values)
            column->insertData(value.data(), value.size());
        chunks.push_back({std::move(column)});
    }
    checkExtractionRoundTrip(header, std::move(chunks), /*max_batch_rows=*/ 2, /*require_extractable_keys=*/ true);
}

TEST(DistinctSetFilterExtraction, SerializedNullableStringKey)
{
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    const Block header = {ColumnWithTypeAndName(type, "k")};

    auto column = type->createColumn();
    column->insert(Field("a"));
    column->insertDefault();
    column->insert(Field("b"));
    column->insert(Field("a"));
    column->insertDefault();
    checkExtractionRoundTrip(header, {{std::move(column)}}, /*max_batch_rows=*/ 2, /*require_extractable_keys=*/ true);
}

TEST(DistinctSetFilterExtraction, ByteTargetSplitsVariableWidthKeys)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "k")};
    std::vector<String> values;
    for (size_t i = 0; i < 100; ++i)
        values.push_back(std::to_string(i) + String(1000 + i, 'x'));

    auto input = makeStringColumn(values);
    const auto expected = collectRows({input}, values.size());
    DistinctSetFilter filter(header, {}, SizeLimits{});
    filter.filter(Chunk({std::move(input)}, values.size()));
    auto extractor = std::move(filter).extractKeys();

    RowsMultiset extracted;
    size_t batch_count = 0;
    while (true)
    {
        const size_t byte_target = batch_count % 2 == 0 ? 4096 : 8192;
        auto batch = extractor->next(1000, byte_target);
        if (batch.empty())
            break;
        ASSERT_EQ(batch.size(), 1);
        const size_t rows = batch.front()->size();
        EXPECT_GT(rows, 0);
        EXPECT_LT(rows, values.size());

        Columns columns;
        columns.push_back(std::move(batch.front()));
        auto current_rows = collectRows(columns, rows);
        extracted.merge(current_rows);
        if (extracted.size() < values.size())
            EXPECT_GE(columns.front()->allocatedBytes(), byte_target);
        ++batch_count;
    }

    EXPECT_GT(batch_count, 1);
    EXPECT_EQ(extracted, expected);
}

TEST(DistinctSetFilterExtraction, KeyLargerThanByteTargetMakesProgress)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "k")};
    const String value(16384, 'x');
    DistinctSetFilter filter(header, {}, SizeLimits{});
    filter.filter(Chunk({makeStringColumn({value})}, 1));
    auto extractor = std::move(filter).extractKeys();

    auto batch = extractor->next(1000, /*max_bytes=*/ 1024);
    ASSERT_EQ(batch.size(), 1);
    ASSERT_EQ(batch.front()->size(), 1);
    EXPECT_GT(batch.front()->allocatedBytes(), 1024);
    EXPECT_TRUE(extractor->next(1000, /*max_bytes=*/ 1024).empty());
    extractor.reset();
    EXPECT_EQ((*batch.front())[0].safeGet<String>(), value);
}

TEST(DistinctSetFilterExtraction, ReturnedColumnsSurviveEarlyExtractorDestruction)
{
    for (const size_t key_count : {1, 2})
    {
        SCOPED_TRACE(key_count);
        auto type = std::make_shared<DataTypeString>();
        Block header = {ColumnWithTypeAndName(type, "a")};
        Columns columns = {makeStringColumn({"first", "second", "third"})};
        if (key_count == 2)
        {
            header.insert(ColumnWithTypeAndName(type, "b"));
            columns.push_back(makeStringColumn({"first suffix", "second suffix", "third suffix"}));
        }

        /// One string uses the string table; two strings use serialized keys backed by the arena.
        DistinctSetFilter filter(header, {}, SizeLimits{}, /*skip_null_keys_=*/ false, /*require_extractable_keys_=*/ true);
        filter.filter(Chunk(std::move(columns), 3));
        auto extractor = std::move(filter).extractKeys();
        auto batch = extractor->next(1, /*max_bytes=*/ 0);
        ASSERT_EQ(batch.size(), key_count);
        ASSERT_EQ(batch.front()->size(), 1);
        extractor.reset();

        const auto key = (*batch.front())[0].safeGet<String>();
        EXPECT_TRUE(key == "first" || key == "second" || key == "third");
        if (key_count == 2)
            EXPECT_EQ((*batch[1])[0].safeGet<String>(), key + " suffix");
    }
}

TEST(DistinctSetFilterGrowth, PreparationAndGrowthEstimationDoNotInsertRows)
{
    constexpr size_t num_rows = 1024;
    for (const bool populated : {false, true})
    {
        SCOPED_TRACE(populated);
        const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")};
        DistinctSetFilter filter(header, {}, SizeLimits{});
        if (populated)
            filter.filter(Chunk({makeColumn({1, 2, 3, 4})}, 4));

        auto column = ColumnUInt64::create();
        column->getData().resize(num_rows);
        iota(column->getData().data(), column->size(), UInt64(0));
        Chunk input(Columns{std::move(column)}, num_rows);
        filter.prepareForInsert(input);

        const size_t prepared_bytes = filter.getTotalByteCount();
        const size_t growth_memory = filter.estimateGrowthMemory(input.getNumRows());
        EXPECT_GT(growth_memory, 0);
        EXPECT_EQ(filter.estimateGrowthMemory(input.getNumRows()), growth_memory);
        EXPECT_EQ(filter.estimateGrowthMemory(0), 0);
        EXPECT_EQ(filter.getTotalByteCount(), prepared_bytes);
        EXPECT_EQ(filter.getTotalRowCount(), populated ? 4 : 0);
        EXPECT_EQ(input.getNumRows(), num_rows);
        EXPECT_EQ(input.getColumns().front()->getUInt(num_rows - 1), num_rows - 1);

        const auto output = filter.filter(std::move(input));
        EXPECT_EQ(output.getNumRows(), num_rows - (populated ? 4 : 0));
        EXPECT_EQ(filter.getTotalRowCount(), num_rows);
    }
}

TEST(DistinctSetFilterGrowth, FixedTablesNeedNoGrowthMemory)
{
    for (const auto & type : DataTypes{std::make_shared<DataTypeUInt8>(), std::make_shared<DataTypeUInt16>()})
    {
        SCOPED_TRACE(type->getName());
        const Block header = {ColumnWithTypeAndName(type, "k")};
        DistinctSetFilter filter(header, {}, SizeLimits{});
        auto column = type->createColumn();
        column->insert(Field(UInt64(1)));
        column->insert(Field(UInt64(2)));
        Chunk input(Columns{std::move(column)}, 2);
        filter.prepareForInsert(input);

        EXPECT_EQ(filter.estimateGrowthMemory(0), 0);
        EXPECT_EQ(filter.estimateGrowthMemory(1048576), 0);
        filter.filter(std::move(input));
        EXPECT_EQ(filter.estimateGrowthMemory(1048576), 0);
    }
}
