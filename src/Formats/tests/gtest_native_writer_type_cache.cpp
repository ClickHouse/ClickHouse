#include <gtest/gtest.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/NativeWriter.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

/// A writer with the map must write the same bytes as a writer without it. Column `a`, and the element of
/// the `Tuple` column `t`, are sparse in the second block only, and the `String` column `r` is replicated in
/// the third block only, so the map must keep one serialization for each kind of the column and of its
/// elements. `a` and `b`
/// have one type object. `s` has the states of a versioned aggregate function, and `f` has the same states
/// under a `SimpleAggregateFunction` type, whose name the writer makes again when it sets the version.
TEST(NativeWriterTypeCache, WritesTheSameBytesAsWithoutTheMap)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 100;
    const DataTypePtr number_type = std::make_shared<DataTypeUInt64>();
    const DataTypePtr state_type = DataTypeFactory::instance().get("AggregateFunction(uniq, UInt64)");
    const auto & state_function = assert_cast<const DataTypeAggregateFunction &>(*state_type).getFunction();
    const DataTypePtr tuple_type = std::make_shared<DataTypeTuple>(DataTypes{number_type});
    const DataTypePtr simple_type = DataTypeFactory::instance().get("SimpleAggregateFunction(anyLast, AggregateFunction(uniq, UInt64))");
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    auto header = std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(number_type->createColumn(), number_type, "a"),
        ColumnWithTypeAndName(number_type->createColumn(), number_type, "b"),
        ColumnWithTypeAndName(state_type->createColumn(), state_type, "s"),
        ColumnWithTypeAndName(tuple_type->createColumn(), tuple_type, "t"),
        ColumnWithTypeAndName(simple_type->createColumn(), simple_type, "f"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "r")});

    auto make_block = [&](bool sparse, bool replicated)
    {
        /// Every tenth row has a value, the other rows have the default 0. A sparse column holds the
        /// default first and then the other values, and the offsets of their rows.
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
        ColumnPtr a = sparse ? ColumnPtr(ColumnSparse::create(std::move(sparse_values), std::move(sparse_offsets), rows))
                             : ColumnPtr(numbers->clone());

        auto states = state_type->createColumn();
        auto & state_column = assert_cast<ColumnAggregateFunction &>(*states);
        const IColumn * arguments[] = {numbers.get()};
        for (size_t row = 0; row < rows; ++row)
        {
            state_column.insertDefault();
            state_function->add(state_column.getData().back(), arguments, row, &state_column.createOrGetArena());
        }
        /// Two strings, one for the even rows and one for the odd rows.
        auto strings = ColumnString::create();
        strings->insert(Field(String("even")));
        strings->insert(Field(String("odd")));
        auto indexes = ColumnUInt8::create();
        for (size_t row = 0; row < rows; ++row)
            indexes->insertValue(static_cast<UInt8>(row % 2));
        ColumnPtr r = ColumnReplicated::create(ColumnPtr(std::move(strings)), ColumnPtr(std::move(indexes)));
        if (!replicated)
            r = r->convertToFullColumnIfReplicated();

        ColumnPtr t = ColumnTuple::create(Columns{a});
        ColumnPtr s = std::move(states);
        return header->cloneWithColumns(Columns{a, std::move(numbers), s, std::move(t), s, std::move(r)});
    };

    const std::vector<Block> blocks{make_block(false, false), make_block(true, false), make_block(false, true)};

    NativeWriterTypeCache type_cache;
    for (const auto & block : blocks)
    {
        WriteBufferFromOwnString without_map;
        NativeWriter(without_map, DBMS_TCP_PROTOCOL_VERSION, header).write(block);
        without_map.finalize();

        WriteBufferFromOwnString with_map;
        NativeWriter(with_map, DBMS_TCP_PROTOCOL_VERSION, header, std::nullopt, false, nullptr, 0, &type_cache).write(block);
        with_map.finalize();

        EXPECT_EQ(with_map.str(), without_map.str());
    }

    ASSERT_EQ(type_cache.entries.size(), 5u);
    EXPECT_EQ(type_cache.entries.at(number_type.get()).serializations.size(), 2u);
    EXPECT_EQ(type_cache.entries.at(string_type.get()).serializations.size(), 2u);
    EXPECT_EQ(type_cache.entries.at(simple_type.get()).serializations.size(), 1u);
    EXPECT_EQ(type_cache.entries.at(state_type.get()).serializations.size(), 1u);
    EXPECT_EQ(type_cache.entries.at(tuple_type.get()).serializations.size(), 2u);
}
