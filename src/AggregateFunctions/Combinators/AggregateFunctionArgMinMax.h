#pragma once

#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{

template <typename Key>
class AggregateFunctionArgMinMax final : public IAggregateFunctionHelper<AggregateFunctionArgMinMax<Key>>
{
private:
    AggregateFunctionPtr nested_function;
    SerializationPtr serialization;
    size_t key_col;
    size_t key_offset;

    Key & key(AggregateDataPtr __restrict place) const { return *reinterpret_cast<Key *>(place + key_offset); }
    const Key & key(ConstAggregateDataPtr __restrict place) const { return *reinterpret_cast<const Key *>(place + key_offset); }

public:
    AggregateFunctionArgMinMax(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionArgMinMax<Key>>{arguments, params, nested_function_->getResultType()}
        , nested_function{nested_function_}
        , serialization(arguments.back()->getDefaultSerialization())
        , key_col{arguments.size() - 1}
        , key_offset{(nested_function->sizeOfData() + alignof(Key) - 1) / alignof(Key) * alignof(Key)}
    {
    }

    String getName() const override { return nested_function->getName() + Key::name(); }

    bool isState() const override { return nested_function->isState(); }

    bool isVersioned() const override { return nested_function->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override { return nested_function->getVersionFromRevision(revision); }

    size_t getDefaultVersion() const override { return nested_function->getDefaultVersion(); }

    bool allocatesMemoryInArena() const override { return nested_function->allocatesMemoryInArena() || Key::allocatesMemoryInArena(); }

    bool hasTrivialDestructor() const override { return nested_function->hasTrivialDestructor(); }

    size_t sizeOfData() const override { return key_offset + sizeof(Key); }

    size_t alignOfData() const override { return nested_function->alignOfData(); }

    void create(AggregateDataPtr __restrict place) const override
    {
        nested_function->create(place);
        new (place + key_offset) Key;
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroy(place); }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override { nested_function->destroyUpToState(place); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (key(place).changeIfBetter(*columns[key_col], row_num, arena))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->add(place, columns, row_num, arena);
        }
        else if (key(place).isEqualTo(*columns[key_col], row_num))
        {
            nested_function->add(place, columns, row_num, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (key(place).changeIfBetter(key(rhs), arena))
        {
            nested_function->destroy(place);
            nested_function->create(place);
            nested_function->merge(place, rhs, arena);
        }
        else if (key(place).isEqualTo(key(rhs)))
        {
            nested_function->merge(place, rhs, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        nested_function->serialize(place, buf, version);
        key(place).write(buf, *serialization);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        nested_function->deserialize(place, buf, version, arena);
        key(place).read(buf, *serialization, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertResultInto(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_function->insertMergeResultInto(place, to, arena);
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_function; }
};

}
