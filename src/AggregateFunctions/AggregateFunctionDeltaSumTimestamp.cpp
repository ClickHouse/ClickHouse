#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
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
        >{arguments, params, createResultType()}
    {}

    AggregationFunctionDeltaSumTimestamp()
        : IAggregateFunctionDataHelper<
            AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType>,
            AggregationFunctionDeltaSumTimestamp<ValueType, TimestampType>
        >{}
    {}

    bool allocatesMemoryInArena() const override { return false; }

    String getName() const override { return "deltaSumTimestamp"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<ValueType>>(); }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = assert_cast<const ColumnVector<ValueType> &>(*columns[0]).getData()[row_num];
        auto ts = assert_cast<const ColumnVector<TimestampType> &>(*columns[1]).getData()[row_num];

        auto & data = this->data(place);

        if ((data.last < value) && data.seen)
        {
            data.sum += (value - data.last);
        }

        data.last = value;
        data.last_ts = ts;

        if (!data.seen)
        {
            data.first = value;
            data.seen = true;
            data.first_ts = ts;
        }
    }

    // before returns true if lhs is before rhs or false if it is not or can't be determined
    bool ALWAYS_INLINE before(
        const AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType> & lhs,
        const AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType> & rhs) const
    {
        if (lhs.last_ts < rhs.first_ts)
            return true;
        if (lhs.last_ts == rhs.first_ts && (lhs.last_ts < rhs.last_ts || lhs.first_ts < rhs.first_ts))
            return true;
        return false;
    }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & place_data = this->data(place);
        auto & rhs_data = this->data(rhs);

        if (!place_data.seen && rhs_data.seen)
        {
            place_data.sum = rhs_data.sum;
            place_data.seen = true;
            place_data.first = rhs_data.first;
            place_data.first_ts = rhs_data.first_ts;
            place_data.last = rhs_data.last;
            place_data.last_ts = rhs_data.last_ts;
        }
        else if (place_data.seen && !rhs_data.seen)
        {
            return;
        }
        else if (before(place_data, rhs_data))
        {
            // This state came before the rhs state

            if (rhs_data.first > place_data.last)
                place_data.sum += (rhs_data.first - place_data.last);
            place_data.sum += rhs_data.sum;
            place_data.last = rhs_data.last;
            place_data.last_ts = rhs_data.last_ts;
        }
        else if (before(rhs_data, place_data))
        {
            // This state came after the rhs state

            if (place_data.first > rhs_data.last)
                place_data.sum += (place_data.first - rhs_data.last);
            place_data.sum += rhs_data.sum;
            place_data.first = rhs_data.first;
            place_data.first_ts = rhs_data.first_ts;
        }
        else
        {
            // If none of those conditions matched, it means both states we are merging have all
            // same timestamps. We have to pick either the smaller or larger value so that the
            // result is deterministic.

            if (place_data.first < rhs_data.first)
            {
                place_data.first = rhs_data.first;
                place_data.last = rhs_data.last;
            }
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & data = this->data(place);
        writeBinaryLittleEndian(data.sum, buf);
        writeBinaryLittleEndian(data.first, buf);
        writeBinaryLittleEndian(data.first_ts, buf);
        writeBinaryLittleEndian(data.last, buf);
        writeBinaryLittleEndian(data.last_ts, buf);
        writeBinaryLittleEndian(data.seen, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & data = this->data(place);
        readBinaryLittleEndian(data.sum, buf);
        readBinaryLittleEndian(data.first, buf);
        readBinaryLittleEndian(data.first_ts, buf);
        readBinaryLittleEndian(data.last, buf);
        readBinaryLittleEndian(data.last_ts, buf);
        readBinaryLittleEndian(data.seen, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<ValueType> &>(to).getData().push_back(this->data(place).sum);
    }
};

AggregateFunctionPtr createAggregateFunctionDeltaSumTimestamp(
    const String & name,
    const DataTypes & arguments,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);
    assertBinary(name, arguments);

    if (!isInteger(arguments[0]) && !isFloat(arguments[0]) && !isDate(arguments[0]) && !isDateTime(arguments[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}, "
                        "must be Int, Float, Date, DateTime", arguments[0]->getName(), name);

    if (!isInteger(arguments[1]) && !isFloat(arguments[1]) && !isDate(arguments[1]) && !isDateTime(arguments[1]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}, "
                        "must be Int, Float, Date, DateTime", arguments[1]->getName(), name);

    return AggregateFunctionPtr(createWithTwoNumericOrDateTypes<AggregationFunctionDeltaSumTimestamp>(
        *arguments[0], *arguments[1], arguments, params));
}
}

void registerAggregateFunctionDeltaSumTimestamp(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("deltaSumTimestamp", { createAggregateFunctionDeltaSumTimestamp, properties });
}

}
