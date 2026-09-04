#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/AlignedBuffer.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

namespace
{

AggregateFunctionPtr getAggregateFunction(const String & name, const DataTypes & arguments)
{
    AggregateFunctionProperties properties;
    return AggregateFunctionFactory::instance().get(name, NullsAction::EMPTY, arguments, {}, properties);
}

void addBitmapValues(const AggregateFunctionPtr & function, AggregateDataPtr place)
{
    auto values = ColumnUInt64::create();
    values->getData().push_back(1);
    values->getData().push_back(2);

    const IColumn * column = values.get();
    for (size_t row = 0; row < values->size(); ++row)
        function->add(place, &column, row, nullptr);
}

void expectLegacyStateRemainsMergeable(const String & name)
{
    DataTypes value_arguments{std::make_shared<DataTypeUInt64>()};
    const auto bitmap_function = getAggregateFunction("groupBitmap", value_arguments);

    AlignedBuffer bitmap_place(bitmap_function->sizeOfData(), bitmap_function->alignOfData());
    bitmap_function->create(bitmap_place.data());
    addBitmapValues(bitmap_function, bitmap_place.data());

    const auto bitmap_column = ColumnAggregateFunction::create(bitmap_function);
    bitmap_column->insertFrom(bitmap_place.data());
    bitmap_function->destroy(bitmap_place.data());

    const auto bitmap_type = std::make_shared<DataTypeAggregateFunction>(bitmap_function, value_arguments, Array{});
    const auto function = getAggregateFunction(name, DataTypes{bitmap_type});

    AlignedBuffer source_place(function->sizeOfData(), function->alignOfData());
    function->create(source_place.data());
    const IColumn * bitmap_column_ptr = bitmap_column.get();
    function->add(source_place.data(), &bitmap_column_ptr, 0, nullptr);

    WriteBufferFromOwnString buffer;
    function->serialize(source_place.data(), buffer, 0);
    function->destroy(source_place.data());

    AlignedBuffer deserialized_place(function->sizeOfData(), function->alignOfData());
    function->create(deserialized_place.data());
    ReadBufferFromString reader(buffer.str());
    function->deserialize(deserialized_place.data(), reader, 0);

    AlignedBuffer result_place(function->sizeOfData(), function->alignOfData());
    function->create(result_place.data());
    function->merge(result_place.data(), deserialized_place.data(), nullptr);

    auto result = function->getResultType()->createColumn();
    function->insertResultInto(result_place.data(), *result, nullptr);
    EXPECT_EQ(assert_cast<const ColumnUInt64 &>(*result).getElement(0), 2);

    function->destroy(result_place.data());
    function->destroy(deserialized_place.data());
}

}

TEST(AggregateFunctionGroupBitmap, LegacyStatesRemainMergeable)
{
    tryRegisterAggregateFunctions();

    expectLegacyStateRemainsMergeable("groupBitmapAnd");
    expectLegacyStateRemainsMergeable("groupBitmapOr");
    expectLegacyStateRemainsMergeable("groupBitmapXor");
}
