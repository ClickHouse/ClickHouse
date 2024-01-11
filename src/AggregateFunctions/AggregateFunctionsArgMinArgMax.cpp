#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/SingleValueData.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/IDataType.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{

extern const int CORRUPTED_DATA;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
}

namespace
{

struct AggregateFunctionArgMinMaxData
{
private:
    SingleValueDataBase::memory_block r_data;
    SingleValueDataBase::memory_block v_data;

public:
    SingleValueDataBase & result() { return r_data.get(); }
    const SingleValueDataBase & result() const { return r_data.get(); }
    SingleValueDataBase & value() { return v_data.get(); }
    const SingleValueDataBase & value() const { return v_data.get(); }

    [[noreturn]] explicit AggregateFunctionArgMinMaxData()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AggregateFunctionArgMinMaxData initialized empty");
    }

    explicit AggregateFunctionArgMinMaxData(TypeIndex result_type, TypeIndex value_type)
    {
        generateSingleValueFromTypeIndex(result_type, r_data);
        generateSingleValueFromTypeIndex(value_type, v_data);
    }
};

static_assert(
    sizeof(AggregateFunctionArgMinMaxData) == 2 * SingleValueDataBase::MAX_STORAGE_SIZE,
    "Incorrect size of AggregateFunctionArgMinMaxData struct");

/// Returns the first arg value found for the minimum/maximum value. Example: argMin(arg, value).
template <bool isMin>
class AggregateFunctionArgMinMax final
    : public IAggregateFunctionDataHelper<AggregateFunctionArgMinMaxData, AggregateFunctionArgMinMax<isMin>>
{
private:
    const DataTypePtr & type_val;
    const SerializationPtr serialization_res;
    const SerializationPtr serialization_val;

    const TypeIndex result_type_index;
    const TypeIndex value_type_index;

    using Base = IAggregateFunctionDataHelper<AggregateFunctionArgMinMaxData, AggregateFunctionArgMinMax<isMin>>;

public:
    AggregateFunctionArgMinMax(const DataTypePtr & type_res_, const DataTypePtr & type_val_)
        : Base({type_res_, type_val_}, {}, type_res_)
        , type_val(this->argument_types[1])
        , serialization_res(type_res_->getDefaultSerialization())
        , serialization_val(type_val->getDefaultSerialization())
        , result_type_index(WhichDataType(type_res_).idx)
        , value_type_index(WhichDataType(type_val_).idx)
    {
        if (!type_val->isComparable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of aggregate function {} because the values of that data type are not comparable",
                type_val->getName(),
                getName());
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) AggregateFunctionArgMinMaxData(result_type_index, value_type_index);
    }

    String getName() const override
    {
        if constexpr (isMin)
            return "argMin";
        else
            return "argMax";
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if constexpr (isMin)
        {
            if (this->data(place).value().setIfSmaller(*columns[1], row_num, arena))
                this->data(place).result().set(*columns[0], row_num, arena);
        }
        else
        {
            if (this->data(place).value().setIfGreater(*columns[1], row_num, arena))
                this->data(place).result().set(*columns[0], row_num, arena);
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        add(place, columns, 0, arena);
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
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndexNotNullIf(*columns[1], nullptr, if_map.data(), row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], nullptr, if_map.data(), row_begin, row_end);
        }
        else
        {
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndex(*columns[1], row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndex(*columns[1], row_begin, row_end);
        }

        if (idx)
            add(place, columns, *idx, arena);
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
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndexNotNullIf(*columns[1], null_map, if_map.data(), row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], null_map, if_map.data(), row_begin, row_end);
        }
        else
        {
            if constexpr (isMin)
                idx = this->data(place).value().getSmallestIndexNotNullIf(*columns[1], null_map, nullptr, row_begin, row_end);
            else
                idx = this->data(place).value().getGreatestIndexNotNullIf(*columns[1], null_map, nullptr, row_begin, row_end);
        }

        if (idx)
            add(place, columns, *idx, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if constexpr (isMin)
        {
            if (this->data(place).value().setIfSmaller(this->data(rhs).value(), arena))
                this->data(place).result().set(this->data(rhs).result(), arena);
        }
        else
        {
            if (this->data(place).value().setIfGreater(this->data(rhs).value(), arena))
                this->data(place).result().set(this->data(rhs).result(), arena);
        }
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

    bool allocatesMemoryInArena() const override { return result_type_index == TypeIndex::String || value_type_index == TypeIndex::String; }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).result().insertResultInto(to);
    }
};

template <bool isMin>
AggregateFunctionPtr
createAggregateFunctionArgMinMax(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];

    return AggregateFunctionPtr(new AggregateFunctionArgMinMax<isMin>(res_type, val_type));
}

}

void registerAggregateFunctionsArgMinArgMax(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("argMin", {createAggregateFunctionArgMinMax<true>, properties});
    factory.registerFunction("argMax", {createAggregateFunctionArgMinMax<false>, properties});
}

}
