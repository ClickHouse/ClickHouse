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
    Field first;
    Field last;
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

        if (this->data(place).count)
        {
            if (this->data(place).last != value)
            {
                this->data(place).count += 1;
                this->data(place).last = value;
            }
        }
        else
        {
            this->data(place).count += 1;
            this->data(place).first = value;
            this->data(place).last = value;
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        if (this->data(place).count && this->data(rhs).count)
        {
            if (this->data(place).last == this->data(rhs).first)
                this->data(place).count += this->data(rhs).count - 1;
            else
                this->data(place).count += this->data(rhs).count;

            this->data(place).last = this->data(rhs).last;
        }
        else if (this->data(rhs).count)
        {
            this->data(place).count = this->data(rhs).count;
            this->data(place).first = this->data(rhs).first;
            this->data(place).last = this->data(rhs).last;
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeIntBinary(this->data(place).count, buf);
        writeIntBinary(this->data(place).first, buf);
        writeIntBinary(this->data(place).last, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readIntBinary(this->data(place).count, buf);
        readIntBinary(this->data(place).first, buf);
        readIntBinary(this->data(place).last, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(this->data(place).count);
    }
};

}
