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
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename T>
struct AggregationFunctionDeltaSumData
{
    T sum = 0;
    T last = 0;
    T first = 0;
    bool seen = false;
};

template <typename T>
class AggregationFunctionDeltaSum final
    : public IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>
{
public:
    AggregationFunctionDeltaSum(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>{arguments, params, createResultType()}
    {}

    AggregationFunctionDeltaSum()
        : IAggregateFunctionDataHelper<AggregationFunctionDeltaSumData<T>, AggregationFunctionDeltaSum<T>>{}
    {}

    String getName() const override { return "deltaSum"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeNumber<T>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];

        if ((this->data(place).last < value) && this->data(place).seen)
        {
            this->data(place).sum += (value - this->data(place).last);
        }

        this->data(place).last = value;

        if (!this->data(place).seen)
        {
            this->data(place).first = value;
            this->data(place).seen = true;
        }
    }

    void NO_SANITIZE_UNDEFINED merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto place_data = &this->data(place);
        auto rhs_data = &this->data(rhs);

        if ((place_data->last < rhs_data->first) && place_data->seen && rhs_data->seen)
        {
            // If the lhs last number seen is less than the first number the rhs saw, the lhs is before
            // the rhs, for example [0, 2] [4, 7]. So we want to add the deltasums, but also add the
            // difference between lhs last number and rhs first number (the 2 and 4). Then we want to
            // take last value from the rhs, so first and last become 0 and 7.

            place_data->sum += rhs_data->sum + (rhs_data->first - place_data->last);
            place_data->last = rhs_data->last;
        }
        else if ((rhs_data->first < place_data->last && rhs_data->seen && place_data->seen))
        {
            // In the opposite scenario, the lhs comes after the rhs, e.g. [4, 6] [1, 2]. Since we
            // assume the input interval states are sorted by time, we assume this is a counter
            // reset, and therefore do *not* add the difference between our first value and the
            // rhs last value.

            place_data->sum += rhs_data->sum;
            place_data->last = rhs_data->last;
        }
        else if (rhs_data->seen && !place_data->seen)
        {
            // If we're here then the lhs is an empty state and the rhs does have some state, so
            // we'll just take that state.

            place_data->first = rhs_data->first;
            place_data->last = rhs_data->last;
            place_data->sum = rhs_data->sum;
            place_data->seen = rhs_data->seen;
        }

        // Otherwise lhs either has data or is uninitialized, so we don't need to modify its values.
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinaryLittleEndian(this->data(place).sum, buf);
        writeBinaryLittleEndian(this->data(place).first, buf);
        writeBinaryLittleEndian(this->data(place).last, buf);
        writeBinaryLittleEndian(this->data(place).seen, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readBinaryLittleEndian(this->data(place).sum, buf);
        readBinaryLittleEndian(this->data(place).first, buf);
        readBinaryLittleEndian(this->data(place).last, buf);
        readBinaryLittleEndian(this->data(place).seen, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).sum);
    }
};

AggregateFunctionPtr createAggregateFunctionDeltaSum(
    const String & name,
    const DataTypes & arguments,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);

    if (arguments.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}", name);

    const DataTypePtr & data_type = arguments[0];

    if (isInteger(data_type) || isFloat(data_type))
        return AggregateFunctionPtr(createWithNumericType<AggregationFunctionDeltaSum>(
            *data_type, arguments, params));
    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}", arguments[0]->getName(), name);
}
}

void registerAggregateFunctionDeltaSum(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("deltaSum", { createAggregateFunctionDeltaSum, properties });
}

}
