#include "config.h"

#if USE_AVRO_RUST

#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/Avro/AvroRustPlan.h>
#include <Processors/Formats/Impl/Avro/AvroRustSchema.h>
#include <base/scope_guard.h>

#include <exception>
#include <thread>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int QUERY_WAS_CANCELLED;
}

/// The items of arrays and maps that `AvroRustPlan` appends to the columns, checked on the columns of crafted rows.
namespace
{

using namespace DB;
using Bytes = std::vector<UInt8>;

/// The budget of items that occupy no bytes of a row (`MAX_ZERO_SIZE_ITEMS` of the decoder).
constexpr UInt64 ZERO_SIZE_ITEMS_PER_ROW = 1 << 24;

void appendLong(Bytes & out, Int64 value)
{
    UInt64 zigzag = (static_cast<UInt64>(value) << 1) ^ static_cast<UInt64>(value >> 63);
    do
    {
        const UInt8 low = zigzag & 0x7F;
        zigzag >>= 7;
        out.push_back(zigzag ? (low | 0x80) : low);
    } while (zigzag);
}

/// An array of `count` items of `item` bytes each: its only block, if it has items, and the end.
void appendArray(Bytes & out, UInt64 count, const Bytes & item = {})
{
    if (count)
        appendLong(out, static_cast<Int64>(count));
    if (!item.empty())
        for (UInt64 i = 0; i < count; ++i)
            out.insert(out.end(), item.begin(), item.end());
    appendLong(out, 0);
}

String recordSchema(const String & fields)
{
    return R"({"type":"record","name":"R","fields":[)" + fields + "]}";
}

String arrayField(const String & name, const String & items)
{
    return R"({"name":")" + name + R"(","type":{"type":"array","items":)" + items + "}}";
}

DataTypePtr nothing()
{
    return std::make_shared<DataTypeNothing>();
}

DataTypePtr nullableNothing()
{
    return std::make_shared<DataTypeNullable>(nothing());
}

DataTypePtr arrayOf(const DataTypePtr & item)
{
    return std::make_shared<DataTypeArray>(item);
}

Block headerOf(std::initializer_list<std::pair<String, DataTypePtr>> columns)
{
    Block header;
    for (const auto & [name, type] : columns)
        header.insert({type->createColumn(), type, name});
    return header;
}

struct Decoded
{
    MutableColumns columns;
    VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>> missing;
};

Decoded decodeRows(
    const String & schema_json, const Block & header, const Bytes & input, UInt64 rows, bool null_as_default = true,
    AvroRust::ValueValidator validator = {})
{
    AvroRust::Schema schema(schema_json);
    AvroRust::Plan plan(header, schema, /*allow_missing_fields=*/false, null_as_default, FormatSettings{}, {}, std::move(validator));
    Decoded result;
    result.columns = header.cloneEmptyColumns();
    plan.decode({input.data(), input.size()}, rows, result.columns, result.missing);
    return result;
}

int codeOfDecodeError(
    const String & schema_json, const Block & header, const Bytes & input, UInt64 rows, bool null_as_default = true,
    AvroRust::ValueValidator validator = {})
{
    try
    {
        decodeRows(schema_json, header, input, rows, null_as_default, std::move(validator));
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    return -1;
}

/// Decodes `rows` rows on a thread of a query that is cancelled from the first time the decode asks, and returns the
/// code of the exception, `-1` if the decode finishes. Nothing of the call may be left in the columns.
int codeOfDecodeCancelled(const String & schema_json, const Block & header, const Bytes & input, UInt64 rows)
{
    AvroRust::Schema schema(schema_json);
    AvroRust::Plan plan(header, schema, /*allow_missing_fields=*/false, /*null_as_default=*/true, FormatSettings{});
    Decoded result;
    result.columns = header.cloneEmptyColumns();
    int code = -1;
    std::thread thread(
        [&]
        {
            ThreadStatus thread_status;
            auto group = std::make_shared<ThreadGroup>(getContext().context, 0);
            group->setQueryCancellationPredicates(
                [] { return true; }, [] { throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Cancelled by the test"); });
            CurrentThread::attachToGroupIfDetached(group);
            SCOPE_EXIT({ CurrentThread::detachFromGroupIfNotDetached(); });
            try
            {
                plan.decode({input.data(), input.size()}, rows, result.columns, result.missing);
            }
            catch (const Exception & e)
            {
                code = e.code();
            }
        });
    thread.join();
    for (const auto & column : result.columns)
        EXPECT_EQ(column->size(), 0);
    return code;
}

}

TEST(AvroRustPlanItems, ArraysOfItemsWithoutDataKeepOnlyTheirLengths)
{
    /// Rows with the whole budget of items that occupy no bytes, and the offsets and the sizes that they must have.
    constexpr UInt64 rows = 4;
    Bytes input;
    for (UInt64 row = 0; row < rows; ++row)
        appendArray(input, ZERO_SIZE_ITEMS_PER_ROW);

    const String empty_record = R"({"type":"record","name":"E","fields":[]})";
    for (const auto & [item_schema, item_type] :
         {std::pair<String, DataTypePtr>{R"("null")", nothing()},
          std::pair<String, DataTypePtr>{empty_record, std::make_shared<DataTypeTuple>(DataTypes{})}})
    {
        const auto result = decodeRows(recordSchema(arrayField("a", item_schema)), headerOf({{"a", arrayOf(item_type)}}), input, rows);
        const auto & array = assert_cast<const ColumnArray &>(*result.columns[0]);
        ASSERT_EQ(array.size(), rows);
        for (UInt64 row = 0; row < rows; ++row)
            EXPECT_EQ(array.getOffsets()[row], (row + 1) * ZERO_SIZE_ITEMS_PER_ROW) << item_type->getName();
        EXPECT_EQ(array.getData().size(), rows * ZERO_SIZE_ITEMS_PER_ROW) << item_type->getName();
        EXPECT_EQ(array.getData().allocatedBytes(), 0) << item_type->getName();
    }
}

TEST(AvroRustPlanItems, DefaultItemsKeepTheirValues)
{
    /// Rows of 0, 3 and 1 items.
    Bytes input;
    for (const UInt64 count : std::initializer_list<UInt64>{0, 3, 1})
        appendArray(input, count);

    {
        const auto result = decodeRows(
            recordSchema(arrayField("a", R"("null")")), headerOf({{"a", arrayOf(nullableNothing())}}), input, 3);
        const auto & array = assert_cast<const ColumnArray &>(*result.columns[0]);
        EXPECT_EQ(array.getOffsets()[0], 0);
        EXPECT_EQ(array.getOffsets()[1], 3);
        EXPECT_EQ(array.getOffsets()[2], 4);
        const auto & nullable = assert_cast<const ColumnNullable &>(array.getData());
        ASSERT_EQ(nullable.size(), 4);
        for (size_t i = 0; i < 4; ++i)
            EXPECT_TRUE(nullable.isNullAt(i));
    }
    {
        /// A record of nulls is a tuple of the defaults of its elements.
        const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{nothing(), nullableNothing()}, Names{"x", "y"});
        const String item = R"({"type":"record","name":"T","fields":[{"name":"x","type":"null"},{"name":"y","type":"null"}]})";
        const auto result = decodeRows(recordSchema(arrayField("a", item)), headerOf({{"a", arrayOf(tuple_type)}}), input, 3);
        const auto & array = assert_cast<const ColumnArray &>(*result.columns[0]);
        EXPECT_EQ(array.getOffsets()[2], 4);
        const auto & tuple = assert_cast<const ColumnTuple &>(array.getData());
        ASSERT_EQ(tuple.size(), 4);
        EXPECT_EQ(tuple.getColumn(0).size(), 4);
        const auto & y = assert_cast<const ColumnNullable &>(tuple.getColumn(1));
        ASSERT_EQ(y.size(), 4);
        for (size_t i = 0; i < 4; ++i)
            EXPECT_TRUE(y.isNullAt(i));
    }
    {
        /// A map written as an array of records, with a null key (the empty string) and a null value.
        const String entry = R"({"type":"record","name":"Entry","fields":[{"name":"key","type":"null"},{"name":"value","type":"null"}]})";
        const auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), nullableNothing());
        const auto result = decodeRows(recordSchema(arrayField("m", entry)), headerOf({{"m", map_type}}), input, 3);
        const auto & map = assert_cast<const ColumnMap &>(*result.columns[0]);
        EXPECT_EQ(map.getNestedColumn().getOffsets()[2], 4);
        const auto & keys = assert_cast<const ColumnString &>(map.getNestedData().getColumn(0));
        ASSERT_EQ(keys.size(), 4);
        EXPECT_EQ(keys.getDataAt(3), "");
        const auto & values = assert_cast<const ColumnNullable &>(map.getNestedData().getColumn(1));
        ASSERT_EQ(values.size(), 4);
        EXPECT_TRUE(values.isNullAt(3));
    }
}

TEST(AvroRustPlanItems, NestedColumnsKeepTheMissingFlagsOfDefaultItems)
{
    /// `n.a` is a `Nullable`: a row with items has its values, a row without has none. `n.b` is not: the null of the
    /// file is replaced by the default (`input_format_null_as_default`), so its values are missing in every row.
    const String item = R"({"type":"record","name":"I","fields":[{"name":"a","type":"null"},{"name":"b","type":"null"}]})";
    const auto header = headerOf({{"n.a", arrayOf(nullableNothing())}, {"n.b", arrayOf(nothing())}});
    Bytes input;
    for (const UInt64 count : std::initializer_list<UInt64>{0, 5, 2})
        appendArray(input, count);

    const auto result = decodeRows(recordSchema(arrayField("n", item)), header, input, 3);
    ASSERT_EQ(result.missing.size(), 2);
    EXPECT_EQ(result.missing[0], (VectorWithMemoryTracking<UInt8>{1, 0, 0}));
    EXPECT_EQ(result.missing[1], (VectorWithMemoryTracking<UInt8>{1, 1, 1}));
    for (const auto & column : result.columns)
    {
        const auto & array = assert_cast<const ColumnArray &>(*column);
        EXPECT_EQ(array.getOffsets()[2], 7);
        EXPECT_EQ(array.getData().size(), 7);
    }
}

TEST(AvroRustPlanItems, DefaultItemsAreStillValidatedAndLimited)
{
    const String schema = recordSchema(arrayField("a", R"("null")"));
    const auto header = headerOf({{"a", arrayOf(nothing())}});
    Bytes input;
    appendArray(input, 3);

    /// A validator that rejects the `null` items: reading one is an error, and an array without items has none.
    const AvroRust::ValueValidator reject_null = [](std::string_view, AvroRust::Node node, size_t) -> std::optional<String>
    {
        if (node.kind() == AVRO_KIND_NULL)
            return String("Null items are not allowed here");
        return std::nullopt;
    };
    EXPECT_EQ(codeOfDecodeError(schema, header, input, 1, true, reject_null), ErrorCodes::INCORRECT_DATA);
    Bytes empty;
    appendArray(empty, 0);
    EXPECT_EQ(codeOfDecodeError(schema, header, empty, 1, true, reject_null), -1);

    /// A `null` that the type cannot hold is an error of the plan unless `input_format_null_as_default`.
    EXPECT_NE(codeOfDecodeError(schema, header, input, 1, /*null_as_default=*/false), -1);
    EXPECT_EQ(codeOfDecodeError(schema, headerOf({{"a", arrayOf(nullableNothing())}}), input, 1, /*null_as_default=*/false), -1);

    /// The budget of items that occupy no bytes of a row.
    Bytes at_limit;
    appendArray(at_limit, ZERO_SIZE_ITEMS_PER_ROW);
    Bytes over_limit;
    appendArray(over_limit, ZERO_SIZE_ITEMS_PER_ROW + 1);
    EXPECT_EQ(codeOfDecodeError(schema, header, at_limit, 1), -1);
    EXPECT_NE(codeOfDecodeError(schema, header, over_limit, 1), -1);
}

TEST(AvroRustPlanItems, CancelledDecodeStopsInTheLoopsOverItemsAndLeavesNoRows)
{
    constexpr UInt64 items = 200000;
    const auto int16 = std::make_shared<DataTypeInt16>();
    const auto string_to_int16 = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), int16);
    const String entry = R"({"type":"record","name":"Entry","fields":[{"name":"key","type":"null"},{"name":"value","type":"int"}]})";
    const String nested_item = R"({"type":"record","name":"I","fields":[{"name":"a","type":"null"},{"name":"b","type":"int"}]})";

    /// Items of one byte each: an int, a map entry with an empty key, an item of a record with one int.
    Bytes one_byte_items;
    appendArray(one_byte_items, items, Bytes{0});

    /// An array that is read item by item, a map, a map written as an array of records, and nested columns.
    EXPECT_EQ(
        codeOfDecodeCancelled(recordSchema(arrayField("a", R"("int")")), headerOf({{"a", arrayOf(int16)}}), one_byte_items, 1),
        ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_EQ(
        codeOfDecodeCancelled(
            recordSchema(R"({"name":"m","type":{"type":"map","values":"null"}})"),
            headerOf({{"m", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), nullableNothing())}}), one_byte_items, 1),
        ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_EQ(
        codeOfDecodeCancelled(recordSchema(arrayField("m", entry)), headerOf({{"m", string_to_int16}}), one_byte_items, 1),
        ErrorCodes::QUERY_WAS_CANCELLED);
    EXPECT_EQ(
        codeOfDecodeCancelled(
            recordSchema(arrayField("n", nested_item)), headerOf({{"n.a", arrayOf(nullableNothing())}, {"n.b", arrayOf(int16)}}),
            one_byte_items, 1),
        ErrorCodes::QUERY_WAS_CANCELLED);
}

#endif
