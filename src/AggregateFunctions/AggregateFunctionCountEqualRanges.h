#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

struct AggregateFunctionCountEqualRangesData
{
    UInt64 count = 0;
    Field value;
};

class AggregateFunctionCountEqualRanges final : public IAggregateFunctionDataHelper<
    AggregateFunctionCountEqualRangesData,
    AggregateFunctionCountEqualRanges>
{
public:
    AggregateFunctionCountEqualRanges(const DataTypes & arguments_, const Array & params_)
        : IAggregateFunctionDataHelper<
            AggregateFunctionCountEqualRangesData,
            AggregateFunctionCountEqualRanges> {arguments_, params_}
    {
        // nothing
    }

    String getName() const override
    {
        return "countEqualRanges";
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const Field & value = (*columns[0])[row_num];

        if (!this->data(place).count || this->data(place).value != value)
        {
            this->data(place).count += 1;
            this->data(place).value = value;
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).count += this->data(rhs).count;

        if (this->data(rhs).count) {
            this->data(place).value = this->data(rhs).value;
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeIntBinary(this->data(place).count, buf);
        writeIntBinary(this->data(place).value, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readIntBinary(this->data(place).count, buf);
        readIntBinary(this->data(place).value, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).count);
    }
};

}
