#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/IDataType.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TDigestSketchData.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnMap.h>

#ifdef USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeNumber<Float64>>(), std::make_shared<DataTypeNumber<Int64>>());
    }

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
        auto centroids = this->data(place).getCentroids();
        
        // Create a Map field which is a vector of (key, value) tuples
        Map map_field;
        for (const auto & [key, value] : centroids)
        {
            Tuple tuple;
            tuple.push_back(Field(key));
            tuple.push_back(Field(value));
            map_field.push_back(Field(tuple));
        }
        
        assert_cast<ColumnMap &>(to).insert(map_field);
    }
};

static AggregateFunctionPtr createAggregateFunctionSerializedTDigest(
    const String & name,
    const DataTypes & argument_types,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);

    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}", name);

    const DataTypePtr & data_type = argument_types[0];

    WhichDataType which(*data_type);
    if (which.isNumber())
        return AggregateFunctionPtr(createWithNumericType<AggregationFunctionSerializedTDigest>(*data_type, argument_types, params));

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}", argument_types[0]->getName(), name);
}

}
void registerAggregateFunctionSerializedTDigest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("serializedTDigest", createAggregateFunctionSerializedTDigest);
}

}

#endif
