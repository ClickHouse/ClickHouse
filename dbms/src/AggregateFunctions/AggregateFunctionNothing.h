#pragma once

#include <DataTypes/DataTypeNothing.h>
#include <Columns/IColumn.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{


/** Aggregate function that takes arbitary number of arbitary arguments and does nothing.
  */
class AggregateFunctionNothing final : public IAggregateFunction
{
public:
    String getName() const override
    {
        return "nothing";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNothing>();
    }

    void setArguments(const DataTypes &) override
    {
    }

    void setParameters(const Array &) override
    {
    }

    void create(AggregateDataPtr) const override
    {
    }

    void destroy(AggregateDataPtr) const noexcept override
    {
    }

    bool hasTrivialDestructor() const override
    {
        return true;
    }

    size_t sizeOfData() const override
    {
        return 0;
    }

    size_t alignOfData() const override
    {
        return 1;
    }

    void add(AggregateDataPtr, const IColumn **, size_t, Arena *) const override
    {
    }

    void merge(AggregateDataPtr, ConstAggregateDataPtr, Arena *) const override
    {
    }

    void serialize(ConstAggregateDataPtr, WriteBuffer &) const override
    {
    }

    void deserialize(AggregateDataPtr, ReadBuffer &, Arena *) const override
    {
    }

    void insertResultInto(ConstAggregateDataPtr, IColumn & to) const override
    {
        to.insertDefault();
    }

    static void addFree(const IAggregateFunction *, AggregateDataPtr, const IColumn **, size_t, Arena *)
    {
    }

    IAggregateFunction::AddFunc getAddressOfAddFunction() const override final { return &addFree; }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
