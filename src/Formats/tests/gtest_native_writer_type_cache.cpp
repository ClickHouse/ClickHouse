#include <gtest/gtest.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/FormatSettings.h>
#include <Formats/NativeWriter.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{

/// Every tenth row has a value, the other rows have the default 0. A sparse column holds the default first
/// and then the other values, and the offsets of their rows.
ColumnPtr makeNumbers(size_t rows, bool sparse)
{
    auto numbers = ColumnUInt64::create();
    auto sparse_values = ColumnUInt64::create();
    auto sparse_offsets = ColumnUInt64::create();
    sparse_values->insertValue(0);
    for (size_t row = 0; row < rows; ++row)
    {
        const UInt64 value = row % 10 == 0 ? row : 0;
        numbers->insertValue(value);
        if (value != 0)
        {
            sparse_values->insertValue(value);
            sparse_offsets->insertValue(row);
        }
    }
    if (sparse)
        return ColumnSparse::create(std::move(sparse_values), std::move(sparse_offsets), rows);
    return numbers;
}

/// Writes `block` with a new writer that has `type_cache` and with a new writer that has no map, and
/// checks that both write the same bytes.
void expectSameBytes(const SharedHeader & header, const Block & block, NativeWriterTypeCache & type_cache)
{
    WriteBufferFromOwnString without_map;
    NativeWriter(without_map, DBMS_TCP_PROTOCOL_VERSION, header).write(block);
    without_map.finalize();

    WriteBufferFromOwnString with_map;
    NativeWriter(with_map, DBMS_TCP_PROTOCOL_VERSION, header, std::nullopt, false, nullptr, 0, &type_cache).write(block);
    with_map.finalize();

    EXPECT_EQ(with_map.str(), without_map.str());
}

}

/// Column `a`, the element of the `Tuple` column `t` and the inner element of the nested `Tuple` column `n`
/// are sparse in the second block only. The `String` column `r`, and the `Tuple` column `p` with a sparse
/// element, are replicated in the third block only. So the map must keep one serialization for each kind of
/// the column and of its elements. `a` and `b` have one type object. `s` has the states of a versioned
/// aggregate function, and `f` has the same states under a `SimpleAggregateFunction` type, whose name the
/// writer makes again when it sets the version.
TEST(NativeWriterTypeCache, WritesTheSameBytesAsWithoutTheMap)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 100;
    const DataTypePtr number_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr state_type = DataTypeFactory::instance().get("AggregateFunction(uniq, UInt64)");
    const auto & state_function = assert_cast<const DataTypeAggregateFunction &>(*state_type).getFunction();
    const DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(DataTypes{number_type});
    const DataTypePtr nested_tuple_type = std::make_shared<DataTypeTuple>(DataTypes{tuple_type});
    const DataTypePtr simple_type = DataTypeFactory::instance().get("SimpleAggregateFunction(anyLast, AggregateFunction(uniq, UInt64))");
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr pair_type = std::make_shared<DataTypeTuple>(DataTypes{number_type, string_type});
    auto header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(number_type->createColumn(), number_type, "a"),
        ColumnWithTypeAndName(number_type->createColumn(), number_type, "b"),
        ColumnWithTypeAndName(state_type->createColumn(), state_type, "s"),
        ColumnWithTypeAndName(tuple_type->createColumn(), tuple_type, "t"),
        ColumnWithTypeAndName(nested_tuple_type->createColumn(), nested_tuple_type, "n"),
        ColumnWithTypeAndName(simple_type->createColumn(), simple_type, "f"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "r"),
        ColumnWithTypeAndName(pair_type->createColumn(), pair_type, "p")});

    auto make_block = [&](bool sparse, bool replicated)
    {
        ColumnPtr a = makeNumbers(rows, sparse);
        ColumnPtr numbers = makeNumbers(rows, false);

        auto states = state_type->createColumn();
        auto & state_column = assert_cast<ColumnAggregateFunction &>(*states);
        const IColumn * arguments[] = {numbers.get()};
        for (size_t row = 0; row < rows; ++row)
        {
            state_column.insertDefault();
            state_function->add(state_column.getData().back(), arguments, row, &state_column.createOrGetArena());
        }

        /// The replicated columns have two different rows: one for the even rows and one for the odd rows.
        auto indexes = ColumnUInt8::create();
        for (size_t row = 0; row < rows; ++row)
            indexes->insertValue(static_cast<UInt8>(row % 2));
        auto strings = ColumnString::create();
        strings->insert(Field(String("even")));
        strings->insert(Field(String("odd")));
        ColumnPtr pair_strings = strings->cloneResized(strings->size());
        ColumnPtr r = ColumnReplicated::create(ColumnPtr(std::move(strings)), ColumnPtr(indexes->cloneResized(rows)));
        /// The `UInt64` element of the two rows is sparse: 0 for the even rows and 7 for the odd rows.
        auto pair_values = ColumnUInt64::create();
        pair_values->insertValue(0);
        pair_values->insertValue(7);
        auto pair_offsets = ColumnUInt64::create();
        pair_offsets->insertValue(1);
        ColumnPtr pair_numbers = ColumnSparse::create(std::move(pair_values), std::move(pair_offsets), 2);
        ColumnPtr p = ColumnReplicated::create(ColumnPtr(ColumnTuple::create(Columns{pair_numbers, pair_strings})), ColumnPtr(std::move(indexes)));
        if (!replicated)
        {
            r = r->convertToFullColumnIfReplicated();
            p = p->convertToFullColumnIfReplicated();
        }

        ColumnPtr t = ColumnTuple::create(Columns{a});
        ColumnPtr n = ColumnTuple::create(Columns{t});
        ColumnPtr s = std::move(states);
        return header->cloneWithColumns(Columns{a, numbers, s, t, n, s, r, p});
    };

    NativeWriterTypeCache type_cache;
    for (const auto & block : {make_block(false, false), make_block(true, false), make_block(false, true)})
        expectSameBytes(header, block, type_cache);

    ASSERT_EQ(type_cache.entries.size(), 7u);
    EXPECT_EQ(type_cache.entries.at(number_type.get()).serializations.size(), 2u);
    EXPECT_EQ(type_cache.entries.at(state_type.get()).serializations.size(), 1u);
    EXPECT_EQ(type_cache.entries.at(tuple_type.get()).serializations.size(), 2u);
    EXPECT_EQ(type_cache.entries.at(nested_tuple_type.get()).serializations.size(), 2u);
    EXPECT_EQ(type_cache.entries.at(simple_type.get()).serializations.size(), 1u);
    EXPECT_EQ(type_cache.entries.at(string_type.get()).serializations.size(), 2u);
    EXPECT_EQ(type_cache.entries.at(pair_type.get()).serializations.size(), 2u);
}

/// A `Tuple` of three numbers has eight kinds: each element is sparse or not. The map keeps the
/// serializations for the first kinds only, and the writer makes the others for each block. The second pass
/// writes all kinds again with another number of rows, and the kept serializations must not change.
TEST(NativeWriterTypeCache, KeepsTheSerializationsForTheFirstKindsOnly)
{
    const DataTypePtr number_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(DataTypes{number_type, number_type, number_type});
    auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(tuple_type->createColumn(), tuple_type, "t")});
    constexpr size_t kinds = 8;
    constexpr size_t limit = NativeWriterTypeCache::max_serializations_per_type;

    NativeWriterTypeCache type_cache;
    std::vector<NativeWriterTypeCache::Serialization> kept;
    for (size_t rows : {50, 70})
    {
        for (size_t sparse_elements = 0; sparse_elements < kinds; ++sparse_elements)
        {
            SCOPED_TRACE(fmt::format("rows={} sparse_elements={}", rows, sparse_elements));
            Columns elements;
            for (size_t element = 0; element < 3; ++element)
                elements.push_back(makeNumbers(rows, (sparse_elements >> element) & 1));
            expectSameBytes(header, header->cloneWithColumns(Columns{ColumnTuple::create(elements)}), type_cache);

            const auto & serializations = type_cache.entries.at(tuple_type.get()).serializations;
            const size_t seen = rows == 50 ? sparse_elements + 1 : kinds;
            ASSERT_EQ(serializations.size(), std::min(seen, limit));
            if (serializations.size() == limit && kept.empty())
                kept = serializations;
        }
    }

    const auto & serializations = type_cache.entries.at(tuple_type.get()).serializations;
    ASSERT_EQ(serializations.size(), kept.size());
    for (size_t i = 0; i < kept.size(); ++i)
    {
        EXPECT_EQ(serializations[i].kinds, kept[i].kinds);
        EXPECT_EQ(serializations[i].serialization, kept[i].serialization);
    }
}

/// The map keeps what the current protocol writes, so a writer that writes the types in another way must not take it.
TEST(NativeWriterTypeCacheDeathTest, RejectsWritersThatChangeTheTypes)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";

    const DataTypePtr number_type = std::make_shared<DataTypeUInt64>();
    auto header = std::make_shared<const Block>(Block{ColumnWithTypeAndName(number_type->createColumn(), number_type, "x")});
    NativeWriterTypeCache type_cache;
    WriteBufferFromOwnString out;

    FormatSettings binary_types;
    binary_types.native.encode_types_in_binary_format = true;

    auto make_writer = [&](UInt64 revision, const std::optional<FormatSettings> & format_settings, bool remove_low_cardinality)
    {
        abort_on_logical_error = true;
        NativeWriter(out, revision, header, format_settings, remove_low_cardinality, nullptr, 0, &type_cache);
    };
    const char * message = "NativeWriter got a type cache";

    EXPECT_DEATH(make_writer(0, std::nullopt, false), message);
    EXPECT_DEATH(make_writer(DBMS_TCP_PROTOCOL_VERSION - 1, std::nullopt, false), message);
    EXPECT_DEATH(make_writer(DBMS_TCP_PROTOCOL_VERSION, std::nullopt, true), message);
    EXPECT_DEATH(make_writer(DBMS_TCP_PROTOCOL_VERSION, binary_types, false), message);
    EXPECT_NO_THROW(NativeWriter(out, DBMS_TCP_PROTOCOL_VERSION, header, std::nullopt, false, nullptr, 0, &type_cache));
    out.finalize();
}
