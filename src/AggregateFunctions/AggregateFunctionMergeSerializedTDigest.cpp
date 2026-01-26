#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/IDataType.h>
#include <Common/StringUtils.h>
#include <Core/Types.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/TDigestSketchData.h>
#include <AggregateFunctions/SketchDataUtils.h>
#include <Columns/ColumnString.h>

#if USE_DATASKETCHES

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
class AggregationFunctionMergeSerializedTDigest final
    : public IAggregateFunctionDataHelper<TDigestSketchData<T>, AggregationFunctionMergeSerializedTDigest<T>>
{
private:
    bool base64_encoded;

public:
    AggregationFunctionMergeSerializedTDigest(const DataTypes & arguments, const Array & params, bool base64_encoded_)
        : IAggregateFunctionDataHelper<TDigestSketchData<T>, AggregationFunctionMergeSerializedTDigest<T>>{arguments, params, createResultType()}
        , base64_encoded(base64_encoded_)
    {}

    AggregationFunctionMergeSerializedTDigest()
        : IAggregateFunctionDataHelper<TDigestSketchData<T>, AggregationFunctionMergeSerializedTDigest<T>>{}
        , base64_encoded(false)
    {}

    String getName() const override { return "mergeSerializedTDigest"; }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeString>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = assert_cast<const ColumnString &>(*columns[0]);
        std::string_view serialized_data = column.getDataAt(row_num);

        if (serialized_data.empty())
            return;

        // Decode if base64, otherwise use raw binary
        std::string decoded_storage;
        auto [data_ptr, data_size] = decodeSketchData(
            serialized_data,
            decoded_storage,
            base64_encoded);

        if (data_ptr == nullptr || data_size == 0)
            return;

        this->data(place).insertSerialized(data_ptr, data_size);
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
        auto serialized_data = this->data(place).getSerializedData();
        assert_cast<ColumnString &>(to).insertData(
            reinterpret_cast<const char*>(serialized_data.data()),
            serialized_data.size());
    }
};

AggregateFunctionPtr createAggregateFunctionMergeSerializedTDigest(
    const String & name,
    const DataTypes & argument_types,
    const Array & params,
    const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments for aggregate function {}", name);

    if (!isString(argument_types[0]))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for aggregate function {} must be String (serialized TDigest sketch)", name);

    bool base64_encoded = false;

    if (params.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} takes at most 1 parameter: base64_encoded (0 or 1)", name);

    if (!params.empty())
    {
        base64_encoded = params[0].safeGet<bool>();
    }

    // Create with Float64 as the default numeric type for TDigest
    return AggregateFunctionPtr(
        new AggregationFunctionMergeSerializedTDigest<Float64>(argument_types, params, base64_encoded));
}

}

void registerAggregateFunctionMergeSerializedTDigest(AggregateFunctionFactory & factory)
{
    factory.registerFunction("mergeSerializedTDigest", createAggregateFunctionMergeSerializedTDigest);
}

}

#endif
