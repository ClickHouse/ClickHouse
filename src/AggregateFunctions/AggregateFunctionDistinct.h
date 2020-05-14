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

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/

class AggregateFunctionDistinct final : public IAggregateFunctionHelper<AggregateFunctionDistinct> {
private:
    mutable std::mutex mutex;
    AggregateFunctionPtr nested_func;
    mutable HashSet<
    UInt128,
    UInt128TrivialHash,
    HashTableGrower<3>,
    HashTableAllocatorWithStackMemory<sizeof(UInt128) * (1 << 3)>> storage;

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested, const DataTypes & arguments)
    : IAggregateFunctionHelper<AggregateFunctionDistinct>(arguments, {})
    , nested_func(nested)
    {
        if (arguments.empty())
            throw Exception("Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    String getName() const override {
        return nested_func->getName() + "Distinct";
    }

    DataTypePtr getReturnType() const override {
        return nested_func->getReturnType();
    }

    void create(AggregateDataPtr place) const override
    {
        nested_func->create(place);
    }

    void destroy(AggregateDataPtr place) const noexcept override {
        nested_func->destroy(place);
    }

    size_t sizeOfData() const override
    {
        return nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return nested_func->alignOfData();
    }

    bool hasTrivialDestructor() const override {
        return nested_func->hasTrivialDestructor();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override {
        UInt128 key;
        SipHash hash;
        columns[0]->updateHashWithValue(row_num, hash);
        hash.get128(key.low, key.high);
        {
            std::lock_guard lock(mutex);
            if (!storage.insert(key).second) {
                return;
            }
        }
        nested_func->add(place, columns, row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override {
        nested_func->merge(place, rhs, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override {
        nested_func->serialize(place, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override {
        nested_func->deserialize(place, buf, arena);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override {
        nested_func->insertResultInto(place, to);
    }

    bool allocatesMemoryInArena() const override {
        return nested_func->allocatesMemoryInArena();
    }
};

}
