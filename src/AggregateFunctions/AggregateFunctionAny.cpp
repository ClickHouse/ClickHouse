#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/SingleValueData.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <base/defines.h>


namespace DB
{
struct Settings;

namespace
{

template <typename Data>
class AggregateFunctionAny final : public IAggregateFunctionDataHelper<Data, AggregateFunctionAny<Data>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionAny(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionAny<Data>>({type}, {}, type), serialization(type->getDefaultSerialization())
    {
    }

    String getName() const override { return "any"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (!this->data(place).has())
            this->data(place).set(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (this->data(place).has() || row_begin >= row_end)
            return;

        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (if_map.data()[i] != 0)
                {
                    this->data(place).set(*columns[0], i, arena);
                    return;
                }
            }
        }
        else
        {
            this->data(place).set(*columns[0], row_begin, arena);
        }
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
        if (this->data(place).has() || row_begin >= row_end)
            return;

        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (if_map.data()[i] != 0 && null_map[i] == 0)
                {
                    this->data(place).set(*columns[0], i, arena);
                    return;
                }
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end; i++)
            {
                if (null_map[i] == 0)
                {
                    this->data(place).set(*columns[0], i, arena);
                    return;
                }
            }
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        if (!this->data(place).has())
            this->data(place).set(*columns[0], 0, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (!this->data(place).has())
            this->data(place).set(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

AggregateFunctionPtr
createAggregateFunctionAny(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionAny>(name, argument_types, parameters, settings));
}


template <typename Data>
class AggregateFunctionAnyLast final : public IAggregateFunctionDataHelper<Data, AggregateFunctionAnyLast<Data>>
{
private:
    SerializationPtr serialization;

public:
    explicit AggregateFunctionAnyLast(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionAnyLast<Data>>({type}, {}, type)
        , serialization(type->getDefaultSerialization())
    {
    }

    String getName() const override { return "anyLast"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).set(*columns[0], row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** __restrict columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        if (row_begin >= row_end)
            return;

        size_t batch_size = row_end - row_begin;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; i++)
            {
                size_t pos = (row_end - 1) - i;
                if (if_map.data()[pos] != 0)
                {
                    this->data(place).set(*columns[0], pos, arena);
                    return;
                }
            }
        }
        else
        {
            this->data(place).set(*columns[0], row_end - 1, arena);
        }
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
        if (row_begin >= row_end)
            return;

        size_t batch_size = row_end - row_begin;
        if (if_argument_pos >= 0)
        {
            const auto & if_map = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = 0; i < batch_size; i++)
            {
                size_t pos = (row_end - 1) - i;
                if (if_map.data()[pos] != 0 && null_map[pos] == 0)
                {
                    this->data(place).set(*columns[0], pos, arena);
                    return;
                }
            }
        }
        else
        {
            for (size_t i = 0; i < batch_size; i++)
            {
                size_t pos = (row_end - 1) - i;
                if (null_map[pos] == 0)
                {
                    this->data(place).set(*columns[0], pos, arena);
                    return;
                }
            }
        }
    }

    void addManyDefaults(AggregateDataPtr __restrict place, const IColumn ** columns, size_t, Arena * arena) const override
    {
        this->data(place).set(*columns[0], 0, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).set(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).read(buf, *serialization, arena);
    }

    bool allocatesMemoryInArena() const override { return Data::allocatesMemoryInArena(); }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        this->data(place).insertResultInto(to);
    }
};

AggregateFunctionPtr createAggregateFunctionAnyLast(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionAnyLast>(name, argument_types, parameters, settings));
}

}

void registerAggregateFunctionsAny(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties default_properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("any", {createAggregateFunctionAny, default_properties});
    factory.registerAlias("any_value", "any", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("first_value", "any", AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("anyLast", {createAggregateFunctionAnyLast, default_properties});
    factory.registerAlias("last_value", "anyLast", AggregateFunctionFactory::CaseInsensitive);
}
}
