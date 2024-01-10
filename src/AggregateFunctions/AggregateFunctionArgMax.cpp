#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/SingleValueData.h>
#include <DataTypes/IDataType.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int CORRUPTED_DATA;
}

namespace
{

/// Returns the first arg value found for the maximum value. Example: argMax(arg, value).
template <typename Data>
class AggregateFunctionArgMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMax<Data>>
{
private:
    const DataTypePtr & type_val;
    const SerializationPtr serialization_res;
    const SerializationPtr serialization_val;

    const TypeIndex result_type;
    const TypeIndex value_type;

    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMax<Data>>;

public:
    AggregateFunctionArgMax(const DataTypePtr & type_res_, const DataTypePtr & type_val_)
        : Base({type_res_, type_val_}, {}, type_res_)
        , type_val(this->argument_types[1])
        , serialization_res(type_res_->getDefaultSerialization())
        , serialization_val(type_val->getDefaultSerialization())
        , result_type(WhichDataType(type_res_).idx)
        , value_type(WhichDataType(type_val_).idx)
    {
        if (!type_val->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of aggregate function {} because the values of that data type are not comparable",
                type_val->getName(),
                getName());
    }

    void create(AggregateDataPtr __restrict place) const override { new (place) Data(result_type, value_type); }

    String getName() const override { return "argMax"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (this->data(place).value().setIfGreater(*columns[1], row_num, arena))
            this->data(place).result().set(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        std::optional<size_t> idx;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], nullptr, if_map.data(), row_begin, row_end);
        }
        else
        {
            idx = this->data(place).value().getGreatestIndex(*columns[1], row_begin, row_end);
        }

        if (idx && this->data(place).value().setIfGreater(*columns[1], *idx, arena))
            this->data(place).result().set(*columns[0], *idx, arena);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        const UInt8 * __restrict null_map,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        std::optional<size_t> idx;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], null_map, if_map.data(), row_begin, row_end);
        }
        else
        {
            idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], null_map, nullptr, row_begin, row_end);
        }

        if (idx && this->data(place).value().setIfGreater(*columns[1], *idx, arena))
            this->data(place).result().set(*columns[0], *idx, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (this->data(place).value().setIfGreater(this->data(rhs).value(), arena))
            this->data(place).result().set(this->data(rhs).result(), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).result().write(buf, *serialization_res);
        this->data(place).value().write(buf, *serialization_val);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).result().read(buf, *serialization_res, arena);
        this->data(place).value().read(buf, *serialization_val, arena);
        if (unlikely(this->data(place).value().has() != this->data(place).result().has()))
            throw Exception(
                ErrorCodes::CORRUPTED_DATA,
                "Invalid state of the aggregate function {}: has_value ({}) != has_result ({})",
                getName(),
                this->data(place).value().has(),
                this->data(place).result().has());
    }

    bool allocatesMemoryInArena() const override { return result_type == TypeIndex::String || value_type == TypeIndex::String; }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).result().insertResultInto(to);
    }
};


AggregateFunctionPtr createAggregateFunctionArgMax(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionArgMax>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionArgMax(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("argMax", {createAggregateFunctionArgMax, properties});
}
}
