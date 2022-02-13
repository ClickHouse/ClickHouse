#pragma once

#include <type_traits>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

template <typename ValueType, typename TimestampType>
struct AggregationFunctionDeltaSumTimestampData
{
    ValueType sum = 0;
    ValueType first = 0;
    ValueType last = 0;
    TimestampType first_ts = 0;
    TimestampType last_ts = 0;
    bool seen = false;
};

template <typename ValueType, typename TimestampType>
class AggregationFunctionDeltaSumTimestamp final
    : public IAggregateFunctionDataHelper<
        AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType>,
        AggregationFunctionDeltaSumTimestamp<ValueType, TimestampType>
      >
{
public:
    AggregationFunctionDeltaSumTimestamp(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<
            AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType>,
            AggregationFunctionDeltaSumTimestamp<ValueType, TimestampType>
        >{arguments, params}
    {}

    AggregationFunctionDeltaSumTimestamp()
        : IAggregateFunctionDataHelper<
            AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType>,
            AggregationFunctionDeltaSumTimestamp<ValueType, TimestampType>
        >{}
    {}

    bool allocatesMemoryInArena() const override { return false; }

    String getName() const override { return "deltaSumTimestamp"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<ValueType>>(); }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = assert_cast<const ColumnVector<ValueType> &>(*columns[0]).getData()[row_num];
        auto ts = assert_cast<const ColumnVector<TimestampType> &>(*columns[1]).getData()[row_num];

        if ((this->data(place).last < value) && this->data(place).seen)
        {
            this->data(place).sum += (value - this->data(place).last);
        }

        this->data(place).last = value;
        this->data(place).last_ts = ts;

        if (!this->data(place).seen)
        {
            this->data(place).first = value;
            this->data(place).seen = true;
            this->data(place).first_ts = ts;
        }
    }

    // before returns true if lhs is before rhs or false if it is not or can't be determined
    bool ALWAYS_INLINE before (
        const AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType> * lhs,
        const AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType> * rhs
    ) const
    {
        if (lhs->last_ts < rhs->first_ts)
        {
            return true;
        }
        if (lhs->last_ts == rhs->first_ts && (lhs->last_ts < rhs->last_ts || lhs->first_ts < rhs->first_ts))
        {
            return true;
        }
        return false;
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto place_data = &this->data(place);
        auto rhs_data = &this->data(rhs);

        if (!place_data->seen && rhs_data->seen)
        {
            place_data->sum = rhs_data->sum;
            place_data->seen = true;
            place_data->first = rhs_data->first;
            place_data->first_ts = rhs_data->first_ts;
            place_data->last = rhs_data->last;
            place_data->last_ts = rhs_data->last_ts;
        }
        else if (place_data->seen && !rhs_data->seen)
            return;
        else if (before(place_data, rhs_data))
        {
            // This state came before the rhs state

            if (rhs_data->first > place_data->last)
                place_data->sum += (rhs_data->first - place_data->last);
            place_data->sum += rhs_data->sum;
            place_data->last = rhs_data->last;
            place_data->last_ts = rhs_data->last_ts;
        }
        else if (before(rhs_data, place_data))
        {
            // This state came after the rhs state

            if (place_data->first > rhs_data->last)
                place_data->sum += (place_data->first - rhs_data->last);
            place_data->sum += rhs_data->sum;
            place_data->first = rhs_data->first;
            place_data->first_ts = rhs_data->first_ts;
        }
        else
        {
            // If none of those conditions matched, it means both states we are merging have all
            // same timestamps. We have to pick either the smaller or larger value so that the
            // result is deterministic.

            if (place_data->first < rhs_data->first)
            {
                place_data->first = rhs_data->first;
                place_data->last = rhs_data->last;
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeIntBinary(this->data(place).sum, buf);
        writeIntBinary(this->data(place).first, buf);
        writeIntBinary(this->data(place).first_ts, buf);
        writeIntBinary(this->data(place).last, buf);
        writeIntBinary(this->data(place).last_ts, buf);
        writePODBinary<bool>(this->data(place).seen, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readIntBinary(this->data(place).sum, buf);
        readIntBinary(this->data(place).first, buf);
        readIntBinary(this->data(place).first_ts, buf);
        readIntBinary(this->data(place).last, buf);
        readIntBinary(this->data(place).last_ts, buf);
        readPODBinary<bool>(this->data(place).seen, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<ValueType> &>(to).getData().push_back(this->data(place).sum);
    }
};

}
