#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>

#include <Common/HashTable/HashSet.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct AggregateFunctionDistinctData
{
    using Key = UInt128;

    HashSet<
        Key,
        UInt128TrivialHash,
        HashTableGrower<3>,
        HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>
    > set;
    std::mutex mutex;

    bool ALWAYS_INLINE tryToInsert(const Key& key)
    {
        return set.insert(key).second;
    }
};

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/

class AggregateFunctionDistinct final : public IAggregateFunctionDataHelper<AggregateFunctionDistinctData, AggregateFunctionDistinct>
{
private:
    AggregateFunctionPtr nested_func;
    size_t num_arguments;
    size_t prefix_size;

    AggregateDataPtr getNestedPlace(AggregateDataPtr place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr place) const noexcept
    {
        return place + prefix_size;
    }

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested, const DataTypes & arguments)
    : IAggregateFunctionDataHelper<AggregateFunctionDistinctData, AggregateFunctionDistinct>(arguments, {})
    , nested_func(nested), num_arguments(arguments.size())
    {
        prefix_size = sizeof(AggregateFunctionDistinctData);

        if (arguments.empty())
            throw Exception("Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    String getName() const override
    {
        return nested_func->getName() + "Distinct";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) AggregateFunctionDistinctData;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        data(place).~AggregateFunctionDistinctData();
        nested_func->destroy(getNestedPlace(place));
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    bool hasTrivialDestructor() const override
    {
        return nested_func->hasTrivialDestructor();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        SipHash hash;
        for (size_t i = 0; i < num_arguments; ++i) {
            columns[i]->updateHashWithValue(row_num, hash);
        }

        UInt128 key;
        hash.get128(key.low, key.high);

        if (this->data(place).tryToInsert(key))
            nested_func->add(getNestedPlace(place), columns, row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        nested_func->merge(getNestedPlace(place), rhs, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        nested_func->serialize(getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        nested_func->deserialize(getNestedPlace(place), buf, arena);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        nested_func->insertResultInto(getNestedPlace(place), to);
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_func->allocatesMemoryInArena();
    }
};

}
