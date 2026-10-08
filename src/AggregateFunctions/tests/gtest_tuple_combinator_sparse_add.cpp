#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/AlignedBuffer.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

/// The per-row `add` of the -Tuple combinator accepts element columns in sparse representation and
/// reads them through the dense values column at the translated index.
TEST(AggregateFunctionTupleCombinator, AddOverSparseElements)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 8;

    /// Element 0, sparse representation of [0, 0, 5, 0, 0, 0, 7, 9].
    auto values = ColumnInt64::create();
    values->getData().push_back(0); /// The shared default value at position 0.
    values->getData().push_back(5);
    values->getData().push_back(7);
    values->getData().push_back(9);
    auto offsets = ColumnUInt64::create();
    offsets->getData().push_back(2);
    offsets->getData().push_back(6);
    offsets->getData().push_back(7);
    ColumnPtr sparse_element = ColumnSparse::create(std::move(values), std::move(offsets), rows);

    /// Element 1, dense: [1.0] * 8.
    auto dense_element = ColumnFloat64::create();
    dense_element->getData().resize_fill(rows, 1.0);

    ColumnPtr tuple = ColumnTuple::create(Columns{sparse_element, std::move(dense_element)});

    DataTypes arguments{std::make_shared<DataTypeTuple>(
        DataTypes{std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeFloat64>()})};
    AggregateFunctionProperties properties;
    AggregateFunctionPtr function
        = AggregateFunctionFactory::instance().get("sumTuple", NullsAction::EMPTY, arguments, {}, properties);

    AlignedBuffer place(function->sizeOfData(), function->alignOfData());
    function->create(place.data());

    const IColumn * column = tuple.get();
    for (size_t row = 0; row < rows; ++row)
        function->add(place.data(), &column, row, nullptr);

    auto result = function->getResultType()->createColumn();
    function->insertResultInto(place.data(), *result, nullptr);
    function->destroy(place.data());

    const auto & result_tuple = assert_cast<const ColumnTuple &>(*result);
    EXPECT_EQ(assert_cast<const ColumnInt64 &>(result_tuple.getColumn(0)).getElement(0), 21);
    EXPECT_EQ(assert_cast<const ColumnFloat64 &>(result_tuple.getColumn(1)).getElement(0), 8.0);
}

/// With multiple zipped Tuple arguments the nested function receives one row index for all of its
/// argument columns, so when any column of an element is sparse, `add` cuts single-row columns
/// (the sparse one from its dense values column at the translated index) and passes row 0.
TEST(AggregateFunctionTupleCombinator, AddOverSparseElementsWithMultipleTuples)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 8;

    /// Values tuple with one element, sparse representation of [0, 0, 5, 0, 0, 0, 7, 9].
    auto values = ColumnFloat64::create();
    values->getData().push_back(0.0); /// The shared default value at position 0.
    values->getData().push_back(5.0);
    values->getData().push_back(7.0);
    values->getData().push_back(9.0);
    auto offsets = ColumnUInt64::create();
    offsets->getData().push_back(2);
    offsets->getData().push_back(6);
    offsets->getData().push_back(7);
    ColumnPtr values_tuple = ColumnTuple::create(Columns{ColumnSparse::create(std::move(values), std::move(offsets), rows)});

    /// Weights tuple with one element, dense: [1.0] * 8.
    auto weights = ColumnFloat64::create();
    weights->getData().resize_fill(rows, 1.0);
    ColumnPtr weights_tuple = ColumnTuple::create(Columns{std::move(weights)});

    DataTypes arguments{
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeFloat64>()}),
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeFloat64>()})};
    AggregateFunctionProperties properties;
    AggregateFunctionPtr function
        = AggregateFunctionFactory::instance().get("avgWeightedTuple", NullsAction::EMPTY, arguments, {}, properties);

    AlignedBuffer place(function->sizeOfData(), function->alignOfData());
    function->create(place.data());

    const IColumn * columns[]{values_tuple.get(), weights_tuple.get()};
    for (size_t row = 0; row < rows; ++row)
        function->add(place.data(), columns, row, nullptr);

    auto result = function->getResultType()->createColumn();
    function->insertResultInto(place.data(), *result, nullptr);
    function->destroy(place.data());

    /// avgWeighted with unit weights: (5 + 7 + 9) / 8.
    const auto & result_tuple = assert_cast<const ColumnTuple &>(*result);
    EXPECT_EQ(assert_cast<const ColumnFloat64 &>(result_tuple.getColumn(0)).getElement(0), 21.0 / 8);
}

/// With multiple zipped Tuple arguments and no sparse columns, `add` passes the element columns
/// through unchanged with the original row index.
TEST(AggregateFunctionTupleCombinator, AddOverDenseElementsWithMultipleTuples)
{
    tryRegisterAggregateFunctions();

    constexpr size_t rows = 8;

    /// Values tuple with one element, dense: [1.0 .. 8.0].
    auto values = ColumnFloat64::create();
    for (size_t i = 0; i < rows; ++i)
        values->getData().push_back(static_cast<Float64>(i + 1));
    ColumnPtr values_tuple = ColumnTuple::create(Columns{std::move(values)});

    /// Weights tuple with one element, dense: [2.0, 1.0, 1.0, ...] so that the result depends on
    /// which value each weight is paired with.
    auto weights = ColumnFloat64::create();
    weights->getData().push_back(2.0);
    for (size_t i = 1; i < rows; ++i)
        weights->getData().push_back(1.0);
    ColumnPtr weights_tuple = ColumnTuple::create(Columns{std::move(weights)});

    DataTypes arguments{
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeFloat64>()}),
        std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeFloat64>()})};
    AggregateFunctionProperties properties;
    AggregateFunctionPtr function
        = AggregateFunctionFactory::instance().get("avgWeightedTuple", NullsAction::EMPTY, arguments, {}, properties);

    AlignedBuffer place(function->sizeOfData(), function->alignOfData());
    function->create(place.data());

    const IColumn * columns[]{values_tuple.get(), weights_tuple.get()};
    for (size_t row = 0; row < rows; ++row)
        function->add(place.data(), columns, row, nullptr);

    auto result = function->getResultType()->createColumn();
    function->insertResultInto(place.data(), *result, nullptr);
    function->destroy(place.data());

    /// (1 * 2 + 2 + 3 + 4 + 5 + 6 + 7 + 8) / (2 + 7 * 1).
    const auto & result_tuple = assert_cast<const ColumnTuple &>(*result);
    EXPECT_EQ(assert_cast<const ColumnFloat64 &>(result_tuple.getColumn(0)).getElement(0), 37.0 / 9);
}
