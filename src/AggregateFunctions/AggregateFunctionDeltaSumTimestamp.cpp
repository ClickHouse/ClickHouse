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

/** Due to a lack of proper code review, this code was contributed with a multiplication of template instantiations
  * over all pairs of data types, and we deeply regret that.
  *
  * We cannot remove all combinations, because the binary representation of serialized data has to remain the same,
  * but we can partially heal the wound by treating unsigned and signed data types in the same way.
  */

template <typename ValueType, typename TimestampType>
struct AggregationFunctionDeltaSumTimestampData
{
    ValueType sum{};
    ValueType first{};
    ValueType last{};
    TimestampType first_ts{};
    TimestampType last_ts{};
    bool seen = false;
};

template <typename ValueType, typename TimestampType>
class AggregationFunctionDeltaSumTimestamp final
    : public IAggregateFunctionDataHelper<
        AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType>,
        AggregationFunctionDeltaSumTimestamp<ValueType, TimestampType>>
{
public:
    AggregationFunctionDeltaSumTimestamp(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<
            AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType>,
            AggregationFunctionDeltaSumTimestamp<ValueType, TimestampType>>{arguments, params, createResultType()}
    {
    }

    AggregationFunctionDeltaSumTimestamp()
        : IAggregateFunctionDataHelper<
            AggregationFunctionDeltaSumTimestampData<ValueType, TimestampType>,
            AggregationFunctionDeltaSumTimestamp<ValueType, TimestampType>>{}
    {
    }

    bool allocatesMemoryInArena() const override { return false; }

    String getName() const override { return "deltaSumTimestamp"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<ValueType>>(); }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = unalignedLoad<ValueType>(columns[0]->getRawData().data() + row_num * sizeof(ValueType));
        auto ts = unalignedLoad<TimestampType>(columns[1]->getRawData().data() + row_num * sizeof(TimestampType));

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
        static_cast<ColumnFixedSizeHelper &>(to).template insertRawData<sizeof(ValueType)>(
            reinterpret_cast<const char *>(&this->data(place).sum));
    }
};


template <typename FirstType, template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
IAggregateFunction * createWithTwoTypesSecond(const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(second_type);

    if (which.idx == TypeIndex::UInt32) return new AggregateFunctionTemplate<FirstType, UInt32>(args...);
    if (which.idx == TypeIndex::UInt64) return new AggregateFunctionTemplate<FirstType, UInt64>(args...);
    if (which.idx == TypeIndex::Int32) return new AggregateFunctionTemplate<FirstType, UInt32>(args...);
    if (which.idx == TypeIndex::Int64) return new AggregateFunctionTemplate<FirstType, UInt64>(args...);
    if (which.idx == TypeIndex::Float32) return new AggregateFunctionTemplate<FirstType, Float32>(args...);
    if (which.idx == TypeIndex::Float64) return new AggregateFunctionTemplate<FirstType, Float64>(args...);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionTemplate<FirstType, UInt16>(args...);
    if (which.idx == TypeIndex::DateTime) return new AggregateFunctionTemplate<FirstType, UInt32>(args...);

    return nullptr;
}

template <template <typename, typename> class AggregateFunctionTemplate, typename... TArgs>
IAggregateFunction * createWithTwoTypes(const IDataType & first_type, const IDataType & second_type, TArgs && ... args)
{
    WhichDataType which(first_type);

    if (which.idx == TypeIndex::UInt8) return createWithTwoTypesSecond<UInt8, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::UInt16) return createWithTwoTypesSecond<UInt16, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::UInt32) return createWithTwoTypesSecond<UInt32, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::UInt64) return createWithTwoTypesSecond<UInt64, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Int8) return createWithTwoTypesSecond<UInt8, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Int16) return createWithTwoTypesSecond<UInt16, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Int32) return createWithTwoTypesSecond<UInt32, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Int64) return createWithTwoTypesSecond<UInt64, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Float32) return createWithTwoTypesSecond<Float32, AggregateFunctionTemplate>(second_type, args...);
    if (which.idx == TypeIndex::Float64) return createWithTwoTypesSecond<Float64, AggregateFunctionTemplate>(second_type, args...);

    return nullptr;
}

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

    auto res = AggregateFunctionPtr(createWithTwoTypes<AggregationFunctionDeltaSumTimestamp>(
        *arguments[0], *arguments[1], arguments, params));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}, "
            "this type is not supported", arguments[0]->getName(), name);

    return res;
}
}

void registerAggregateFunctionDeltaSumTimestamp(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("deltaSumTimestamp", { createAggregateFunctionDeltaSumTimestamp, properties });
}

}
