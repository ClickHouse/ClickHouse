#include "config.h"

#if USE_AVRO_RUST

#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
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

#include <atomic>
#include <exception>
#include <functional>
#include <optional>
#include <thread>
#include <vector>

namespace DB::ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int QUERY_WAS_CANCELLED;
}

/// What `AvroRustPlan.cpp` does with the items of the arrays and maps of a row, checked on the columns that the plan
/// produces for crafted rows (no container file around them): the items that occupy no bytes are appended in constant
/// time, and every other per-item loop looks at the cancellation of the query once per 65536 items.
namespace
{

using namespace DB;
using Bytes = std::vector<UInt8>;

constexpr UInt64 ITEMS_PER_CANCELLATION_CHECK = 1 << 16;
/// The budget of items that occupy no bytes that one row has (`MAX_ZERO_SIZE_ITEMS` of the decoder).
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

/// A block of `count` items without bytes and the end of the array.
void appendZeroWidthArray(Bytes & out, UInt64 count)
{
    if (count)
        appendLong(out, static_cast<Int64>(count));
    appendLong(out, 0);
}

/// A block of `count` items of `item` bytes each and the end of the array (or the map).
void appendArrayOf(Bytes & out, UInt64 count, const Bytes & item)
{
    if (count)
        appendLong(out, static_cast<Int64>(count));
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

String emptyRecord()
{
    return R"({"type":"record","name":"E","fields":[]})";
}

DataTypePtr nothing()
{
    return std::make_shared<DataTypeNothing>();
}

DataTypePtr arrayOf(const DataTypePtr & item)
{
    return std::make_shared<DataTypeArray>(item);
}

DataTypePtr nullableNothing()
{
    return std::make_shared<DataTypeNullable>(nothing());
}

Block headerOf(std::initializer_list<std::pair<String, DataTypePtr>> columns)
{
    Block header;
    for (const auto & [name, type] : columns)
        header.insert({type->createColumn(), type, name});
    return header;
}

using Missing = VectorWithMemoryTracking<VectorWithMemoryTracking<UInt8>>;

struct Decoded
{
    MutableColumns columns;
    Missing missing;
    UInt64 consumed = 0;
};

Decoded decodeRows(
    const String & schema_json,
    const Block & header,
    const Bytes & input,
    UInt64 rows,
    bool null_as_default = true,
    AvroRust::ValueValidator validator = {})
{
    AvroRust::Schema schema(schema_json);
    AvroRust::Plan plan(header, schema, /*allow_missing_fields=*/false, null_as_default, FormatSettings{}, {}, std::move(validator));
    Decoded result;
    result.columns = header.cloneEmptyColumns();
    result.consumed = plan.decode({input.data(), input.size()}, rows, result.columns, result.missing);
    return result;
}

/// The code of the exception that `decodeRows` throws, `-1` if it does not throw.
int codeOfDecodeError(const String & schema_json, const Block & header, const Bytes & input, UInt64 rows, bool null_as_default = true,
                      AvroRust::ValueValidator validator = {}, String * message = nullptr)
{
    try
    {
        decodeRows(schema_json, header, input, rows, null_as_default, std::move(validator));
    }
    catch (const Exception & e)
    {
        if (message)
            *message = e.message();
        return e.code();
    }
    return -1;
}

/// How a query tells its threads that it was cancelled: the number of times the code asked, and the call on which the
/// answer is "cancelled" (0: never).
struct CancellationProbe
{
    std::atomic<size_t> checks = 0;
    size_t cancel_at = 0;
};

/// Runs `body` on a thread of a query whose cancellation check is `probe`, the way `IRowInputFormat::read` and the Avro
/// container reader see it (`CurrentThread::checkIfNotCancelled`), and rethrows what `body` throws.
void runInQuery(CancellationProbe & probe, const std::function<void()> & body)
{
    std::exception_ptr error;
    std::thread thread(
        [&]
        {
            try
            {
                ThreadStatus thread_status;
                auto group = std::make_shared<ThreadGroup>(getContext().context, 0);
                group->setQueryCancellationPredicates(
                    [] { return false; },
                    [&probe]
                    {
                        if (++probe.checks == probe.cancel_at)
                            throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Query was cancelled by the test");
                    });
                CurrentThread::attachToGroupIfDetached(group);
                SCOPE_EXIT({ CurrentThread::detachFromGroupIfNotDetached(); });
                body();
            }
            catch (...)
            {
                error = std::current_exception();
            }
        });
    thread.join();
    if (error)
        std::rethrow_exception(error);
}

/// Decodes `rows` rows and returns how many times the decode looked at the cancellation of the query. With
/// `cancel_at` the decode must throw `QUERY_WAS_CANCELLED` on that look and leave nothing in the columns.
size_t countCancellationChecks(
    const String & schema_json, const Block & header, const Bytes & input, UInt64 rows, size_t cancel_at = 0, Decoded * decoded = nullptr)
{
    CancellationProbe probe;
    probe.cancel_at = cancel_at;
    AvroRust::Schema schema(schema_json);
    AvroRust::Plan plan(header, schema, /*allow_missing_fields=*/false, /*null_as_default=*/true, FormatSettings{});
    Decoded local;
    Decoded & result = decoded ? *decoded : local;
    result.columns = header.cloneEmptyColumns();
    bool cancelled = false;
    try
    {
        runInQuery(
            probe,
            [&]
            {
                result.consumed = plan.decode({input.data(), input.size()}, rows, result.columns, result.missing);
            });
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.message();
        cancelled = true;
        for (const auto & column : result.columns)
            EXPECT_EQ(column->size(), 0) << "A cancelled decode must leave no row of its call in the columns";
    }
    EXPECT_EQ(cancelled, cancel_at != 0 && probe.checks >= cancel_at);
    return probe.checks;
}

}

TEST(AvroRustPlanItems, ZeroWidthItemsOfSelectedArraysAreAppendedInConstantTime)
{
    /// 8192 rows with 2^24 items each, the whole budget of a row: 137438953472 items for 40960 bytes of input. A call
    /// of a reader for each item would be that many calls, which is what an extended query would spend its time on.
    constexpr UInt64 rows = 8192;
    Bytes input;
    for (UInt64 row = 0; row < rows; ++row)
        appendZeroWidthArray(input, ZERO_SIZE_ITEMS_PER_ROW);
    ASSERT_EQ(input.size(), 40960);

    const String nulls = arrayField("a", R"("null")");
    const String empty_records = arrayField("a", emptyRecord());

    for (const auto & [schema_json, item_type] :
         {std::pair<String, DataTypePtr>{recordSchema(nulls), nothing()},
          std::pair<String, DataTypePtr>{recordSchema(empty_records), std::make_shared<DataTypeTuple>(DataTypes{})}})
    {
        const auto result = decodeRows(schema_json, headerOf({{"a", arrayOf(item_type)}}), input, rows);
        EXPECT_EQ(result.consumed, input.size());

        const auto & array = assert_cast<const ColumnArray &>(*result.columns[0]);
        ASSERT_EQ(array.size(), rows);
        for (UInt64 row = 0; row < rows; ++row)
            ASSERT_EQ(array.getOffsets()[row], (row + 1) * ZERO_SIZE_ITEMS_PER_ROW) << item_type->getName() << " row " << row;
        EXPECT_EQ(array.getData().size(), rows * ZERO_SIZE_ITEMS_PER_ROW) << item_type->getName();
        /// The items have no data: nothing grows with them.
        EXPECT_EQ(array.getData().allocatedBytes(), 0) << item_type->getName();

        /// The arrays of the file always have their items, so no row has a missing value (the column is tracked, or not).
        for (const auto & flags : result.missing)
            for (const UInt8 flag : flags)
                EXPECT_EQ(flag, 0);
    }
}

TEST(AvroRustPlanItems, ZeroWidthItemsAreNotVisitedAndDoNotLookAtTheCancellation)
{
    /// A loop over the items would look at the query once per 65536 of them, 256 times per row.
    Bytes input;
    appendZeroWidthArray(input, ZERO_SIZE_ITEMS_PER_ROW);
    appendZeroWidthArray(input, 0);
    const auto header = headerOf({{"a", arrayOf(nothing())}});
    EXPECT_EQ(countCancellationChecks(recordSchema(arrayField("a", R"("null")")), header, input, 2), 0);
}

TEST(AvroRustPlanItems, ZeroWidthItemsOfDefaultsKeepTheirValuesAndMissingFlags)
{
    /// Rows of 0, 3 and 1 items of every kind of default-only item that the plan appends at once.
    Bytes input;
    for (const UInt64 count : std::initializer_list<UInt64>{0, 3, 1})
        appendZeroWidthArray(input, count);

    {
        /// `null` of a `Nullable`: the value is the Avro null, which the file has.
        const auto result = decodeRows(
            recordSchema(arrayField("a", R"("null")")), headerOf({{"a", arrayOf(nullableNothing())}}), input, 3);
        const auto & array = assert_cast<const ColumnArray &>(*result.columns[0]);
        ASSERT_EQ(array.getOffsets().size(), 3);
        EXPECT_EQ(array.getOffsets()[0], 0);
        EXPECT_EQ(array.getOffsets()[1], 3);
        EXPECT_EQ(array.getOffsets()[2], 4);
        const auto & nullable = assert_cast<const ColumnNullable &>(array.getData());
        ASSERT_EQ(nullable.size(), 4);
        ASSERT_EQ(nullable.getNestedColumn().size(), 4);
        for (size_t i = 0; i < 4; ++i)
            EXPECT_TRUE(nullable.isNullAt(i));
    }
    {
        /// A record of nulls is a tuple of defaults, of elements that `Nothing` and `Nullable` have.
        const auto tuple_type = std::make_shared<DataTypeTuple>(DataTypes{nothing(), nullableNothing()}, Names{"x", "y"});
        const String item = R"({"type":"record","name":"T","fields":[{"name":"x","type":"null"},{"name":"y","type":"null"}]})";
        const auto result = decodeRows(recordSchema(arrayField("a", item)), headerOf({{"a", arrayOf(tuple_type)}}), input, 3);
        const auto & array = assert_cast<const ColumnArray &>(*result.columns[0]);
        EXPECT_EQ(array.getOffsets()[0], 0);
        EXPECT_EQ(array.getOffsets()[1], 3);
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
        /// A union of one branch has the value of its branch, and the tag of the branch is one byte.
        Bytes tagged;
        appendArrayOf(tagged, 3, Bytes{0});
        const auto result = decodeRows(
            recordSchema(arrayField("a", R"(["null"])")), headerOf({{"a", arrayOf(nullableNothing())}}), tagged, 1);
        EXPECT_EQ(result.consumed, tagged.size());
        const auto & array = assert_cast<const ColumnArray &>(*result.columns[0]);
        EXPECT_EQ(array.getOffsets()[0], 3);
        EXPECT_TRUE(assert_cast<const ColumnNullable &>(array.getData()).isNullAt(2));
    }
}

TEST(AvroRustPlanItems, NestedColumnsKeepTheMissingFlagOfTheItemsTheyAppendAtOnce)
{
    /// `n.a` is a `Nullable`: a row with items has its values, a row without has none. `n.b` is not: the null of the
    /// file is replaced by the default (`input_format_null_as_default`), so the value is missing in every row.
    const String item = R"({"type":"record","name":"I","fields":[{"name":"a","type":"null"},{"name":"b","type":"null"}]})";
    const auto header = headerOf({{"n.a", arrayOf(nullableNothing())}, {"n.b", arrayOf(nothing())}});
    Bytes input;
    for (const UInt64 count : std::initializer_list<UInt64>{0, 5, ZERO_SIZE_ITEMS_PER_ROW})
        appendZeroWidthArray(input, count);

    const auto result = decodeRows(recordSchema(arrayField("n", item)), header, input, 3);
    ASSERT_EQ(result.missing.size(), 2);
    ASSERT_EQ(result.missing[0].size(), 3);
    EXPECT_EQ(result.missing[0][0], 1);
    EXPECT_EQ(result.missing[0][1], 0);
    EXPECT_EQ(result.missing[0][2], 0);
    ASSERT_EQ(result.missing[1].size(), 3);
    EXPECT_EQ(result.missing[1][0], 1);
    EXPECT_EQ(result.missing[1][1], 1);
    EXPECT_EQ(result.missing[1][2], 1);

    for (const auto & column : result.columns)
    {
        const auto & array = assert_cast<const ColumnArray &>(*column);
        EXPECT_EQ(array.getOffsets()[0], 0);
        EXPECT_EQ(array.getOffsets()[1], 5);
        EXPECT_EQ(array.getOffsets()[2], 5 + ZERO_SIZE_ITEMS_PER_ROW);
        EXPECT_EQ(array.getData().size(), 5 + ZERO_SIZE_ITEMS_PER_ROW);
    }
}

TEST(AvroRustPlanItems, MapEntriesWithoutValuesAreAppendedAtOnce)
{
    /// A map written as an array of records, with a null key (replaced by the empty string) and a null value.
    const String entry = R"({"type":"record","name":"Entry","fields":[{"name":"key","type":"null"},{"name":"value","type":"null"}]})";
    const auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), nullableNothing());
    constexpr UInt64 count = 100000;
    Bytes input;
    appendZeroWidthArray(input, count);
    appendZeroWidthArray(input, 0);

    const auto result = decodeRows(recordSchema(arrayField("m", entry)), headerOf({{"m", map_type}}), input, 2);
    const auto & map = assert_cast<const ColumnMap &>(*result.columns[0]);
    ASSERT_EQ(map.size(), 2);
    EXPECT_EQ(map.getNestedColumn().getOffsets()[0], count);
    EXPECT_EQ(map.getNestedColumn().getOffsets()[1], count);
    const auto & keys = assert_cast<const ColumnString &>(map.getNestedData().getColumn(0));
    ASSERT_EQ(keys.size(), count);
    EXPECT_EQ(keys.getDataAt(0), "");
    EXPECT_EQ(keys.getDataAt(count - 1), "");
    const auto & values = assert_cast<const ColumnNullable &>(map.getNestedData().getColumn(1));
    ASSERT_EQ(values.size(), count);
    EXPECT_TRUE(values.isNullAt(0));
    EXPECT_TRUE(values.isNullAt(count - 1));
}

TEST(AvroRustPlanItems, ValidatorStillRejectsTheItemsThatItRejects)
{
    /// The validator rejects the `null` items when the readers are created, so that reading one is an error: the items
    /// that have no data are not appended without reading them.
    const AvroRust::ValueValidator reject_null = [](std::string_view, AvroRust::Node node, size_t) -> std::optional<String>
    {
        if (node.kind() == AVRO_KIND_NULL)
            return String("Null items are not allowed here");
        return std::nullopt;
    };
    const String schema = recordSchema(arrayField("a", R"("null")"));
    const auto header = headerOf({{"a", arrayOf(nothing())}});

    for (const UInt64 count : std::initializer_list<UInt64>{1, 3, ZERO_SIZE_ITEMS_PER_ROW})
    {
        Bytes input;
        appendZeroWidthArray(input, count);
        String message;
        EXPECT_EQ(codeOfDecodeError(schema, header, input, 1, true, reject_null, &message), ErrorCodes::INCORRECT_DATA) << count;
        EXPECT_NE(message.find("Null items are not allowed here"), String::npos) << message;
    }

    /// An array without items has nothing to reject.
    Bytes input;
    appendZeroWidthArray(input, 0);
    EXPECT_EQ(codeOfDecodeError(schema, header, input, 1, true, reject_null), -1);
}

TEST(AvroRustPlanItems, NullAsDefaultStillDecidesWhetherANullItemIsAnError)
{
    const String schema = recordSchema(arrayField("a", R"("null")"));
    Bytes input;
    appendZeroWidthArray(input, 4);

    /// A `null` that no value of the type can take is an error of the plan, whatever the items are.
    EXPECT_THROW(decodeRows(schema, headerOf({{"a", arrayOf(nothing())}}), input, 1, /*null_as_default=*/false), Exception);
    /// A type that holds the null takes it as it is.
    const auto result = decodeRows(schema, headerOf({{"a", arrayOf(nullableNothing())}}), input, 1, /*null_as_default=*/false);
    EXPECT_EQ(assert_cast<const ColumnArray &>(*result.columns[0]).getData().size(), 4);
}

TEST(AvroRustPlanItems, BudgetOfZeroSizeItemsOfARowIsStillEnforced)
{
    const String schema = recordSchema(arrayField("a", R"("null")") + "," + arrayField("b", R"("null")"));
    const auto header = headerOf({{"a", arrayOf(nothing())}, {"b", arrayOf(nothing())}});
    const auto row = [](UInt64 a, UInt64 b)
    {
        Bytes input;
        appendZeroWidthArray(input, a);
        appendZeroWidthArray(input, b);
        return input;
    };

    EXPECT_EQ(codeOfDecodeError(schema, header, row(ZERO_SIZE_ITEMS_PER_ROW / 2, ZERO_SIZE_ITEMS_PER_ROW / 2), 1), -1);
    EXPECT_NE(codeOfDecodeError(schema, header, row(ZERO_SIZE_ITEMS_PER_ROW, 1), 1), -1);
    EXPECT_NE(codeOfDecodeError(schema, header, row(ZERO_SIZE_ITEMS_PER_ROW + 1, 0), 1), -1);
    /// The budget is that of each row, not of the call.
    Bytes two_rows = row(ZERO_SIZE_ITEMS_PER_ROW, 0);
    const auto second = row(ZERO_SIZE_ITEMS_PER_ROW, 0);
    two_rows.insert(two_rows.end(), second.begin(), second.end());
    EXPECT_EQ(codeOfDecodeError(schema, header, two_rows, 2), -1);
}

TEST(AvroRustPlanItems, ArrayLoopLooksAtTheCancellationOncePerChunkOfItems)
{
    /// `Array(Int16)` is read item by item: the items are 1 byte each.
    const String schema = recordSchema(arrayField("a", R"("int")"));
    const auto header = headerOf({{"a", arrayOf(std::make_shared<DataTypeInt16>())}});
    const auto input_of = [](UInt64 count)
    {
        Bytes input;
        appendArrayOf(input, count, Bytes{0});
        return input;
    };

    EXPECT_EQ(countCancellationChecks(schema, header, input_of(1), 1), 0);
    EXPECT_EQ(countCancellationChecks(schema, header, input_of(ITEMS_PER_CANCELLATION_CHECK), 1), 0);
    EXPECT_EQ(countCancellationChecks(schema, header, input_of(ITEMS_PER_CANCELLATION_CHECK + 1), 1), 1);
    EXPECT_EQ(countCancellationChecks(schema, header, input_of(200000), 1), 3);

    /// A cancelled query stops at the look, and the call leaves nothing in the columns.
    Decoded decoded;
    EXPECT_EQ(countCancellationChecks(schema, header, input_of(200000), 1, /*cancel_at=*/2, &decoded), 2);
}

TEST(AvroRustPlanItems, ArrayLoopOfDefaultItemsThatCannotBeAppendedAtOnceLooksAtTheCancellation)
{
    /// `LowCardinality` has its own reader of the item, which a null replaces by a value of the dictionary type, so it is
    /// not appended with `insertDefaults`: the items are visited, and each chunk of them is a look at the query.
    const auto low_cardinality = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    const String schema = recordSchema(arrayField("a", R"("null")"));
    const auto header = headerOf({{"a", arrayOf(low_cardinality)}});
    Bytes input;
    appendZeroWidthArray(input, 200000);

    Decoded decoded;
    EXPECT_EQ(countCancellationChecks(schema, header, input, 1, 0, &decoded), 3);
    const auto & array = assert_cast<const ColumnArray &>(*decoded.columns[0]);
    ASSERT_EQ(array.getData().size(), 200000);
    EXPECT_TRUE(array.getData().isNullAt(0));
    EXPECT_TRUE(array.getData().isNullAt(199999));

    Decoded cancelled;
    EXPECT_EQ(countCancellationChecks(schema, header, input, 1, /*cancel_at=*/3, &cancelled), 3);
}

TEST(AvroRustPlanItems, MapLoopLooksAtTheCancellationOncePerChunkOfEntries)
{
    /// `map<null>`: 200000 entries of an empty key (1 byte) and a value without bytes.
    const String schema = recordSchema(R"({"name":"m","type":{"type":"map","values":"null"}})");
    const auto header = headerOf({{"m", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), nullableNothing())}});
    Bytes input;
    appendArrayOf(input, 200000, Bytes{0});

    Decoded decoded;
    EXPECT_EQ(countCancellationChecks(schema, header, input, 1, 0, &decoded), 3);
    const auto & map = assert_cast<const ColumnMap &>(*decoded.columns[0]);
    EXPECT_EQ(map.getNestedColumn().getOffsets()[0], 200000);
    EXPECT_EQ(map.getNestedData().getColumn(1).size(), 200000);

    Decoded cancelled;
    EXPECT_EQ(countCancellationChecks(schema, header, input, 1, /*cancel_at=*/1, &cancelled), 1);
}

TEST(AvroRustPlanItems, MapAsArrayOfRecordsVisitsOnlyTheFieldsThatReadSomething)
{
    /// The key is a `null` that the plan appends at once, the value is an `int` that is read for each entry.
    const String entry = R"({"type":"record","name":"Entry","fields":[{"name":"key","type":"null"},{"name":"value","type":"int"}]})";
    const auto header = headerOf({{"m", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt16>())}});
    Bytes input;
    appendArrayOf(input, 200000, Bytes{4});

    Decoded decoded;
    EXPECT_EQ(countCancellationChecks(recordSchema(arrayField("m", entry)), header, input, 1, 0, &decoded), 3);
    const auto & map = assert_cast<const ColumnMap &>(*decoded.columns[0]);
    ASSERT_EQ(map.getNestedData().getColumn(0).size(), 200000);
    ASSERT_EQ(map.getNestedData().getColumn(1).size(), 200000);
    EXPECT_EQ(assert_cast<const ColumnString &>(map.getNestedData().getColumn(0)).getDataAt(199999), "");
    EXPECT_EQ(map.getNestedData().getColumn(1).getInt(0), 2);
    EXPECT_EQ(map.getNestedData().getColumn(1).getInt(199999), 2);

    Decoded cancelled;
    EXPECT_EQ(countCancellationChecks(recordSchema(arrayField("m", entry)), header, input, 1, /*cancel_at=*/2, &cancelled), 2);
}

TEST(AvroRustPlanItems, NestedColumnsLoopLooksAtTheCancellationOncePerChunkOfItems)
{
    /// `n.a` is appended at once (a `null`), `n.b` is read for each item.
    const String item = R"({"type":"record","name":"I","fields":[{"name":"a","type":"null"},{"name":"b","type":"int"}]})";
    const auto header = headerOf({{"n.a", arrayOf(nullableNothing())}, {"n.b", arrayOf(std::make_shared<DataTypeInt16>())}});
    Bytes input;
    appendArrayOf(input, 200000, Bytes{6});

    Decoded decoded;
    EXPECT_EQ(countCancellationChecks(recordSchema(arrayField("n", item)), header, input, 1, 0, &decoded), 3);
    const auto & a = assert_cast<const ColumnArray &>(*decoded.columns[0]);
    const auto & b = assert_cast<const ColumnArray &>(*decoded.columns[1]);
    EXPECT_EQ(a.getData().size(), 200000);
    EXPECT_EQ(b.getData().size(), 200000);
    EXPECT_EQ(b.getData().getInt(199999), 3);
    ASSERT_EQ(decoded.missing.size(), 2);
    EXPECT_EQ(decoded.missing[0][0], 0);

    Decoded cancelled;
    EXPECT_EQ(countCancellationChecks(recordSchema(arrayField("n", item)), header, input, 1, /*cancel_at=*/1, &cancelled), 1);
}

#endif
