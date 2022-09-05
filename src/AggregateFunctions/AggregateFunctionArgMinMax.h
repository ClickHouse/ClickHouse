#pragma once

#include <AggregateFunctions/AggregateFunctionMinMax.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/IDataType.h>
#include <base/StringRef.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


/// For possible values for template parameters, see 'AggregateFunctionMinMaxAny.h'.
template <typename ResultData, typename ValueData>
struct AggregateFunctionArgMinMaxData
{
    using ResultData_t = ResultData;
    using ValueData_t = ValueData;

    ResultData result;  // the argument at which the minimum/maximum value is reached.
    ValueData value;    // value for which the minimum/maximum is calculated.

    static bool allocatesMemoryInArena() { return ResultData::allocates_memory_in_arena || ValueData::allocates_memory_in_arena; }
};

/// Returns the first arg value found for the minimum/maximum value. Example: argMax(arg, value).
template <typename Data>
class AggregateFunctionArgMinMax final : public IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data>>
{
private:
    const DataTypePtr & type_res;
    const DataTypePtr & type_val;
    const SerializationPtr serialization_res;
    const SerializationPtr serialization_val;

    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionArgMinMax<Data>>;

public:
    AggregateFunctionArgMinMax(const DataTypePtr & type_res_, const DataTypePtr & type_val_)
        : Base({type_res_, type_val_}, {})
        , type_res(this->argument_types[0])
        , type_val(this->argument_types[1])
        , serialization_res(type_res->getDefaultSerialization())
        , serialization_val(type_val->getDefaultSerialization())
    {
        if (!type_val->isComparable())
            throw Exception("Illegal type " + type_val->getName() + " of second argument of aggregate function " + getName()
                + " because the values of that data type are not comparable", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    String getName() const override { return StringRef(Data::ValueData_t::ComparatorType::name) == StringRef("min") ? "argMin" : "argMax"; }

    DataTypePtr getReturnType() const override
    {
        return type_res;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (this->data(place).value.add(*columns[1], row_num, arena))
            this->data(place).result.set(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end, AggregateDataPtr place, const IColumn ** columns, Arena * arena, ssize_t if_argument_pos)
        const override
    {
        std::optional<size_t> idx{};
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            idx = this->data(place).value.addManyConditionalIndex(*columns[1], flags.data(), row_begin, row_end, arena);
        }
        else
        {
            idx = this->data(place).value.addManyIndex(*columns[1], row_begin, row_end, arena);
        }
        if (idx)
            this->data(place).result.set(*columns[0], *idx, arena);
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos = -1) const override
    {
        std::optional<size_t> idx{};

        if (if_argument_pos >= 0)
        {
            const auto * if_flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData().data();
            auto final_null_flags = std::make_unique<UInt8[]>(row_end);
            for (size_t i = row_begin; i < row_end; ++i)
                final_null_flags[i] = null_map[i] & !if_flags[i];

            idx = this->data(place).value.addManyNotNullIndex(*columns[1], final_null_flags.get(), row_begin, row_end, arena);
        }
        else
        {
            idx = this->data(place).value.addManyNotNullIndex(*columns[1], null_map, row_begin, row_end, arena);
        }
        if (idx)
            this->data(place).result.set(*columns[0], *idx, arena);
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        if (this->data(place).value.add(*columns[1], 0, arena))
            this->data(place).result.set(*columns[0], 0, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (this->data(place).value.merge(this->data(rhs).value, arena))
            this->data(place).result.set(this->data(rhs).result, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).result.write(buf, *serialization_res);
        this->data(place).value.write(buf, *serialization_val);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).result.read(buf, *serialization_res, arena);
        this->data(place).value.read(buf, *serialization_val, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return Data::allocatesMemoryInArena();
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).result.insertResultInto(to);
    }
};

}
