#include "config.h"

#if USE_ARROW

#include <gtest/gtest.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <Formats/FormatSettings.h>
#include <Processors/Chunk.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/table.h>
#include <arrow/util/key_value_metadata.h>

#include <memory>
#include <stdexcept>
#include <utility>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int DUPLICATE_COLUMN;
extern const int INCORRECT_DATA;
}

namespace
{

template <typename Builder, typename Value>
std::shared_ptr<arrow::Array> makeArray(const std::vector<Value> & values)
{
    Builder builder;
    for (const auto & value : values)
    {
        const auto status = builder.Append(value);
        if (!status.ok())
            throw std::runtime_error(status.ToString());
    }

    auto result = builder.Finish();
    if (!result.ok())
        throw std::runtime_error(result.status().ToString());
    return *std::move(result);
}

std::shared_ptr<arrow::Array> makeNullableInt64Array()
{
    arrow::Int64Builder builder;
    if (const auto status = builder.Append(7); !status.ok())
        throw std::runtime_error(status.ToString());
    if (const auto status = builder.AppendNull(); !status.ok())
        throw std::runtime_error(status.ToString());
    auto result = builder.Finish();
    if (!result.ok())
        throw std::runtime_error(result.status().ToString());
    return *std::move(result);
}

std::shared_ptr<arrow::Array> makeDictionaryArray()
{
    auto indices = makeArray<arrow::Int8Builder, Int8>({0, 1});
    auto dictionary = makeArray<arrow::StringBuilder, String>({"red", "blue"});
    auto result = arrow::DictionaryArray::FromArrays(indices, dictionary);
    if (!result.ok())
        throw std::runtime_error(result.status().ToString());
    return *std::move(result);
}

std::shared_ptr<arrow::RecordBatch> makeCompositeBatch()
{
    auto nested_id = makeArray<arrow::Int64Builder, Int64>({1, 2});
    auto nested_name = makeArray<arrow::StringBuilder, String>({"one", "two"});
    auto nested_result = arrow::StructArray::Make({nested_id, nested_name}, {"id", "name"});
    if (!nested_result.ok())
        throw std::runtime_error(nested_result.status().ToString());

    auto nullable_value = makeNullableInt64Array();
    auto dictionary_value = makeDictionaryArray();
    auto schema = arrow::schema({
        arrow::field("nested", (*nested_result)->type(), /* nullable */ false),
        arrow::field("nullable_value", arrow::int64(), /* nullable */ true),
        arrow::field("dictionary_value", dictionary_value->type(), /* nullable */ false),
    });
    return arrow::RecordBatch::Make(
        std::move(schema),
        2,
        {*std::move(nested_result), std::move(nullable_value), std::move(dictionary_value)});
}

Block makeHeader(std::initializer_list<std::pair<String, String>> columns)
{
    Block header;
    for (const auto & [name, type_name] : columns)
    {
        auto type = DataTypeFactory::instance().get(type_name);
        header.insert({type->createColumn(), type, name});
    }
    return header;
}

ArrowColumnToCHColumn makeConverter(const Block & header)
{
    return ArrowColumnToCHColumn(
        header,
        "ArrowColumnToCHColumnRecordBatch",
        FormatSettings{},
        /* parquet_columns_to_clickhouse */ std::nullopt,
        /* clickhouse_columns_to_parquet */ std::nullopt,
        /* allow_missing_columns */ false,
        /* null_as_default */ false,
        FormatSettings::DateTimeOverflowBehavior::Ignore,
        /* allow_geoparquet_parser */ false,
        /* case_insensitive_matching */ false,
        /* is_stream */ true,
        /* enable_json_parsing */ false);
}

std::shared_ptr<arrow::RecordBatch> makeIntBatch(
    std::shared_ptr<arrow::Schema> schema,
    const std::vector<Int64> & values)
{
    return arrow::RecordBatch::Make(
        std::move(schema),
        static_cast<int64_t>(values.size()),
        {makeArray<arrow::Int64Builder, Int64>(values)});
}

void expectSchemaMismatch(
    const std::shared_ptr<arrow::RecordBatch> & first,
    const std::shared_ptr<arrow::RecordBatch> & second,
    const Block & header)
{
    auto converter = makeConverter(header);
    std::ignore = converter.arrowRecordBatchToCHChunk(*first, nullptr);
    try
    {
        std::ignore = converter.arrowRecordBatchToCHChunk(*second, nullptr);
        FAIL() << "Expected schema mismatch";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::INCORRECT_DATA);
    }
}

}

TEST(ArrowColumnToCHColumnRecordBatch, MatchesTableForNestedNullableAndDictionary)
{
    const auto header = makeHeader({
        {"nested", "Tuple(id Int64, name String)"},
        {"nullable_value", "Nullable(Int64)"},
        {"dictionary_value", "LowCardinality(String)"},
    });
    auto record_batch = makeCompositeBatch();

    auto record_batch_converter = makeConverter(header);
    auto record_batch_chunk = record_batch_converter.arrowRecordBatchToCHChunk(*record_batch, nullptr);

    auto table_result = arrow::Table::FromRecordBatches({record_batch});
    ASSERT_TRUE(table_result.ok()) << table_result.status().ToString();
    auto table_converter = makeConverter(header);
    auto table_chunk = table_converter.arrowTableToCHChunk(*table_result, (*table_result)->num_rows(), nullptr);

    ASSERT_EQ(record_batch_chunk.getNumRows(), 2);
    ASSERT_EQ(record_batch_chunk.getNumColumns(), 3);
    ASSERT_EQ(table_chunk.getNumRows(), record_batch_chunk.getNumRows());
    ASSERT_EQ(table_chunk.getNumColumns(), record_batch_chunk.getNumColumns());
    for (size_t column_index = 0; column_index < record_batch_chunk.getNumColumns(); ++column_index)
    {
        const auto & record_batch_column = record_batch_chunk.getColumns()[column_index];
        const auto & table_column = table_chunk.getColumns()[column_index];
        for (size_t row_index = 0; row_index < record_batch_chunk.getNumRows(); ++row_index)
        {
            Field record_batch_value;
            Field table_value;
            record_batch_column->get(row_index, record_batch_value);
            table_column->get(row_index, table_value);
            EXPECT_EQ(record_batch_value, table_value);
        }
    }
}

TEST(ArrowColumnToCHColumnRecordBatch, ReusesFieldMappingAcrossBatches)
{
    const auto header = makeHeader({{"id", "Int64"}});
    auto converter = makeConverter(header);
    const auto schema = arrow::schema({arrow::field("id", arrow::int64(), /* nullable */ false)});

    EXPECT_FALSE(converter.hasRecordBatchFieldMapping());
    auto first = converter.arrowRecordBatchToCHChunk(*makeIntBatch(schema, {1, 2}), nullptr);
    EXPECT_TRUE(converter.hasRecordBatchFieldMapping());
    auto second = converter.arrowRecordBatchToCHChunk(*makeIntBatch(schema, {3, 4}), nullptr);
    EXPECT_TRUE(converter.hasRecordBatchFieldMapping());

    EXPECT_EQ(first.getNumRows(), 2);
    EXPECT_EQ(second.getNumRows(), 2);
    Field first_value;
    Field second_value;
    second.getColumns()[0]->get(0, first_value);
    second.getColumns()[0]->get(1, second_value);
    EXPECT_EQ(first_value.safeGet<Int64>(), 3);
    EXPECT_EQ(second_value.safeGet<Int64>(), 4);
}

TEST(ArrowColumnToCHColumnRecordBatch, RejectsDuplicateFields)
{
    const auto header = makeHeader({{"id", "Int64"}});
    auto converter = makeConverter(header);
    auto array = makeArray<arrow::Int64Builder, Int64>({1});
    auto batch = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::int64(), /* nullable */ false),
            arrow::field("id", arrow::int64(), /* nullable */ false),
        }),
        1,
        {array, array});

    try
    {
        std::ignore = converter.arrowRecordBatchToCHChunk(*batch, nullptr);
        FAIL() << "Expected duplicate field rejection";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::DUPLICATE_COLUMN);
    }
    EXPECT_FALSE(converter.hasRecordBatchFieldMapping());
}

TEST(ArrowColumnToCHColumnRecordBatch, RejectsSchemaChanges)
{
    const auto single_header = makeHeader({{"id", "Int64"}});
    const auto first_schema = arrow::schema({arrow::field("id", arrow::int64(), /* nullable */ false)});
    const auto first = makeIntBatch(first_schema, {1});

    auto type_changed_array = makeArray<arrow::Int32Builder, Int32>({1});
    auto type_changed = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("id", arrow::int32(), /* nullable */ false)}), 1, {type_changed_array});
    expectSchemaMismatch(first, type_changed, single_header);

    auto nullable_changed = makeIntBatch(
        arrow::schema({arrow::field("id", arrow::int64(), /* nullable */ true)}), {1});
    expectSchemaMismatch(first, nullable_changed, single_header);

    auto metadata_changed = makeIntBatch(
        arrow::schema(
            {arrow::field("id", arrow::int64(), /* nullable */ false)},
            arrow::key_value_metadata({"key"}, {"value"})),
        {1});
    expectSchemaMismatch(first, metadata_changed, single_header);

    const auto reordered_header = makeHeader({{"id", "Int64"}, {"other", "Int64"}});
    auto id = makeArray<arrow::Int64Builder, Int64>({1});
    auto other = makeArray<arrow::Int64Builder, Int64>({2});
    auto ordered = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("id", arrow::int64(), /* nullable */ false),
            arrow::field("other", arrow::int64(), /* nullable */ false),
        }),
        1,
        {id, other});
    auto reordered = arrow::RecordBatch::Make(
        arrow::schema({
            arrow::field("other", arrow::int64(), /* nullable */ false),
            arrow::field("id", arrow::int64(), /* nullable */ false),
        }),
        1,
        {other, id});
    expectSchemaMismatch(ordered, reordered, reordered_header);
}

TEST(ArrowColumnToCHColumnRecordBatch, RejectsHeaderChanges)
{
    auto header = makeHeader({{"id", "Int64"}});
    auto converter = makeConverter(header);
    const auto schema = arrow::schema({arrow::field("id", arrow::int64(), /* nullable */ false)});
    auto batch = makeIntBatch(schema, {1});
    std::ignore = converter.arrowRecordBatchToCHChunk(*batch, nullptr);

    header.erase("id");
    try
    {
        std::ignore = converter.arrowRecordBatchToCHChunk(*batch, nullptr);
        FAIL() << "Expected header mismatch";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::INCORRECT_DATA);
    }
}

TEST(ArrowColumnToCHColumnRecordBatch, HandlesEmptyAndZeroColumnBatches)
{
    const auto int_header = makeHeader({{"id", "Int64"}});
    auto int_converter = makeConverter(int_header);
    auto empty = makeIntBatch(
        arrow::schema({arrow::field("id", arrow::int64(), /* nullable */ false)}), {});
    auto empty_chunk = int_converter.arrowRecordBatchToCHChunk(*empty, nullptr);
    EXPECT_EQ(empty_chunk.getNumRows(), 0);
    EXPECT_EQ(empty_chunk.getNumColumns(), 1);
    EXPECT_TRUE(int_converter.hasRecordBatchFieldMapping());

    const Block zero_column_header;
    auto zero_column_converter = makeConverter(zero_column_header);
    auto zero_column_batch = arrow::RecordBatch::Make(arrow::schema({}), 3, arrow::ArrayVector{});
    auto zero_column_chunk = zero_column_converter.arrowRecordBatchToCHChunk(*zero_column_batch, nullptr);
    EXPECT_EQ(zero_column_chunk.getNumRows(), 3);
    EXPECT_EQ(zero_column_chunk.getNumColumns(), 0);
    EXPECT_TRUE(zero_column_converter.hasRecordBatchFieldMapping());
}

#endif
