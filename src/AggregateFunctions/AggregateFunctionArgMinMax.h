#pragma once

#include <AggregateFunctions/AggregateFunctionMinMaxAny.h> // SingleValueDataString used in embedded compiler
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <common/StringRef.h>
#include "Columns/IColumn.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// For possible values for template parameters, see AggregateFunctionMinMaxAny.h
template <typename ResultData, typename ValueData>
struct AggregateFunctionArgMinMaxData
{
    using ResultData_t = ResultData;
    using ValueData_t = ValueData;

    ResultData result; // the argument at which the minimum/maximum value is reached.
    ValueData value; // value for which the minimum/maximum is calculated.

    static bool allocatesMemoryInArena() { return ResultData::allocatesMemoryInArena() || ValueData::allocatesMemoryInArena(); }

    static String name() { return StringRef(ValueData_t::name()) == StringRef("min") ? "argMin" : "argMax"; }
};

/// Returns the first arg value found for the minimum/maximum value. Example: argMax(arg, value).
template <typename Data>
class AggregateFunctionArgMinMax final : public IAggregateFunctionTupleArgHelper<Data, AggregateFunctionArgMinMax<Data>, 2>
{
private:
    const DataTypePtr & type_res;
    const DataTypePtr & type_val;
    bool tuple_argument;

    using Base = IAggregateFunctionTupleArgHelper<Data, AggregateFunctionArgMinMax<Data>, 2>;

public:
    AggregateFunctionArgMinMax(const DataTypePtr & type_res_, const DataTypePtr & type_val_, const bool tuple_argument_)
        : Base({type_res_, type_val_}, {}, tuple_argument_)
        , type_res(this->argument_types[0])
        , type_val(this->argument_types[1])
    {
        if (!type_val->isComparable())
            throw Exception(
                "Illegal type " + type_val->getName() + " of second argument of aggregate function " + getName()
                    + " because the values of that data type are not comparable",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        this->tuple_argument = tuple_argument_;
    }

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        if (tuple_argument)
        {
            return std::make_shared<DataTypeTuple>(DataTypes{this->type_res, this->type_val});
        }

        return type_res;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (this->data(place).value.changeIfBetter(*columns[1], row_num, arena))
            this->data(place).result.change(*columns[0], row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (this->data(place).value.changeIfBetter(this->data(rhs).value, arena))
            this->data(place).result.change(this->data(rhs).result, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).result.write(buf, *type_res);
        this->data(place).value.write(buf, *type_val);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).result.read(buf, *type_res, arena);
        this->data(place).value.read(buf, *type_val, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        if (tuple_argument)
        {
            auto & tup = assert_cast<ColumnTuple &>(to);

            this->data(place).result.insertResultInto(tup.getColumn(0));
            this->data(place).value.insertResultInto(tup.getColumn(1));
        }
        else
            this->data(place).result.insertResultInto(to);
    }
};


}
