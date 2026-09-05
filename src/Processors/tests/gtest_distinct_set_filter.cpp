#include <gtest/gtest.h>

#include <set>

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
    const Block & header, std::vector<Columns> chunks, size_t max_batch_rows = 1000, bool require_extractable_keys = false)
{
    DistinctSetFilter filter(header, /*columns=*/ {}, SizeLimits{}, /*skip_null_keys_=*/ false, require_extractable_keys);

    RowsMultiset emitted;
    size_t emitted_count = 0;
    for (auto & columns : chunks)
    {
        const size_t num_rows = columns.front()->size();
        Chunk filtered = filter.filter(Chunk(std::move(columns), num_rows));
        if (filtered.hasRows())
        {
            emitted_count += filtered.getNumRows();
            auto rows = collectRows(filtered.getColumns(), filtered.getNumRows());
            emitted.merge(rows);
        }
    }

    ASSERT_TRUE(filter.supportsKeyExtraction());

    RowsMultiset extracted;
    size_t extracted_count = 0;
    for (auto & batch : filter.extractKeyColumns(max_batch_rows))
    {
        const size_t num_rows = batch.front()->size();
        EXPECT_LE(num_rows, max_batch_rows);
        extracted_count += num_rows;

        Columns columns;
        for (auto & column : batch)
            columns.push_back(std::move(column));
        auto rows = collectRows(columns, num_rows);
        extracted.merge(rows);
    }

    EXPECT_EQ(extracted_count, emitted_count);
    EXPECT_EQ(extracted_count, filter.getTotalRowCount());
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
    auto column = ColumnFixedString::create(4);
    for (const auto * value : {"aaaa", "bbbb", "aaaa", "cc\0d"})
        column->insertData(value, 4);

    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeFixedString>(4), "k")};
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

    auto batches = filter.extractKeyColumns(1000);
    ASSERT_EQ(batches.size(), 1u);
    const auto & extracted = assert_cast<const ColumnFloat64 &>(*batches[0][0]).getData();
    ASSERT_EQ(extracted.size(), 2u);
    EXPECT_NE(std::signbit(extracted[0]), std::signbit(extracted[1]));
}

TEST(DistinctSetFilterExtraction, HashedMethodDoesNotSupportExtraction)
{
    /// Two variable-width key columns fall back to the `hashed` method, which is irreversible.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "b")};

    DistinctSetFilter filter(header, {}, SizeLimits{});
    Chunk filtered = filter.filter(Chunk({makeStringColumn({"x", "y"}), makeStringColumn({"u", "v"})}, 2));
    ASSERT_EQ(filtered.getNumRows(), 2u);

    EXPECT_FALSE(filter.supportsKeyExtraction());
}

TEST(DistinctSetFilterSemantics, ThrowModeAllowsReachingTheLimitExactly)
{
    const Block header = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt64>(), "k")};

    SizeLimits limits(/*max_rows=*/ 2, /*max_bytes=*/ 0, OverflowMode::THROW);
    DistinctSetFilter filter(header, {}, limits);

    /// The 'throw' mode uses a strict comparison: a set of exactly max_rows keys is allowed.
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
    /// The LowCardinality fast path must produce exactly the same distinct rows as the generic hash
    /// path over the equal data.
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
    /// UInt8 + UInt16 pack into 3 bytes -> the keys32 method, with the prepared-keys reordering
    /// (the wider column is packed first).
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeUInt8>(), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt16>(), "b")};

    checkExtractionRoundTrip(
        header,
        {{makeNumberColumn<ColumnUInt8, UInt8>({1, 2, 1, 255}), makeNumberColumn<ColumnUInt16, UInt16>({7, 7, 40000, 40000})}});
}

TEST(DistinctSetFilterExtraction, Keys256MultipleColumns)
{
    /// Three UInt64 columns pack into 24 bytes -> the keys256 method.
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

    /// Two nullable UInt64 columns: 16 bytes of values plus the null bitmap -> nullable_keys256.
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

    /// A nullable and a plain column together -> nullable_keys128 with a non-nullable member,
    /// exercising the per-column nullability branch of the unpacking.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()), "a"),
           ColumnWithTypeAndName(std::make_shared<DataTypeUInt32>(), "b")};

    checkExtractionRoundTrip(
        header,
        {{make_nullable_column({1, {}, 1, {}}), makeNumberColumn<ColumnUInt32, UInt32>({5, 5, 6, 6})}});
}

namespace
{

/// A LowCardinality(String) column of `num_rows` rows whose dictionary holds at least `dictionary_size`
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

TEST(DistinctSetFilterSemantics, LowCardinalityBitmapsCountTowardsTheByteSize)
{
    /// The LowCardinality fast path keeps a bitmap of the seen indices per dictionary. Its size is that
    /// of the dictionary, not of the data seen, so a few rows over a large dictionary occupy far more
    /// than their keys in the set, and the reported size must include it.
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

TEST(DistinctSetFilterExtraction, SerializedKeysOnRequest)
{
    /// Two String keys fall to the generic `hashed` method, which cannot materialize the keys back; a
    /// consumer that needs them gets the `serialized` method instead, which stores them.
    const Block header
        = {ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "a"), ColumnWithTypeAndName(std::make_shared<DataTypeString>(), "b")};
    checkExtractionRoundTrip(
        header,
        {{makeStringColumn({"a", "b", "a"}), makeStringColumn({"x", "y", "x"})}, {makeStringColumn({"a", "c"}), makeStringColumn({"y", "z"})}},
        /*max_batch_rows=*/ 1000,
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
    checkExtractionRoundTrip(header, std::move(chunks), /*max_batch_rows=*/ 1000, /*require_extractable_keys=*/ true);
}

TEST(DistinctSetFilterExtraction, SerializedNullableStringKey)
{
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());
    const Block header = {ColumnWithTypeAndName(type, "k")};

    auto column = type->createColumn();
    column->insert(Field("a"));
    column->insertDefault(); /// NULL
    column->insert(Field("b"));
    column->insert(Field("a"));
    column->insertDefault(); /// NULL
    checkExtractionRoundTrip(header, {{std::move(column)}}, /*max_batch_rows=*/ 1000, /*require_extractable_keys=*/ true);
}
