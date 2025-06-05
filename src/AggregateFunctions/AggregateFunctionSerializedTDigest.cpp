#include <type_traits>
#include <experimental/type_traits>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TDigestSketchData.h>
#include <Columns/ColumnString.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{
template <typename T>
class AggregationFunctionSerializedTDigest final
    : public IAggregateFunctionDataHelper<TDigestSketchData<T>, AggregationFunctionSerializedTDigest<T>>
{
public:
    AggregationFunctionSerializedTDigest(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<TDigestSketchData<T>, AggregationFunctionSerializedTDigest<T>>{arguments, params, createResultType()}
    {}

    AggregationFunctionSerializedTDigest()
        : IAggregateFunctionDataHelper<TDigestSketchData<T>, AggregationFunctionSerializedTDigest<T>>{}
    {}

    String getName() const override { return "serializedTDigest"; }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeString>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = *columns[0];
        this->data(place).insertOriginal(column.getFloat64(row_num));
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnString &>(to).insertData(this->data(place).serializedData().c_str(),this->data(place).serializedData().size());
    }
};

AggregateFunctionPtr createAggregateFunctionSerializedTDigest(
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
        return AggregateFunctionPtr(createWithNumericType<AggregationFunctionSerializedTDigest>(
            *data_type, arguments, params));
    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}", arguments[0]->getName(), name);
}

}
void registerAggregateFunctionSerializedTDigest(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("serializedTDigest", {createAggregateFunctionSerializedTDigest, properties});
}

}
