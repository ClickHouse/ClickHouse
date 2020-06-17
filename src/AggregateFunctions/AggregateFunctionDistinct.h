#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
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

template <typename T>
struct AggregateFunctionDistinctSingleNumericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    Set value;
};

template <typename Data, typename Derived>
class AggregateFunctionDistinctBase : public IAggregateFunctionDataHelper<Data, Derived>
{
protected:
    static constexpr size_t prefix_size = sizeof(Data);
    AggregateFunctionPtr nested_func;
    size_t num_arguments;
    

    AggregateDataPtr getNestedPlace(AggregateDataPtr place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr place) const noexcept
    {
        return place + prefix_size;
    }

public:

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroy(getNestedPlace(place));
    }

    String getName() const override
    {
        return nested_func->getName() + "Distinct";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    AggregateFunctionDistinctBase(AggregateFunctionPtr nested, const DataTypes & arguments)
    : IAggregateFunctionDataHelper<Data, Derived>(arguments, {})
    , nested_func(nested), num_arguments(arguments.size())
    {
        if (arguments.empty())
            throw Exception("Aggregate function " + getName() + " require at least one argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
};


/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/
template <typename T>
class AggregateFunctionDistinctSingleNumericImpl final
    : public AggregateFunctionDistinctBase<AggregateFunctionDistinctSingleNumericData<T>,
        AggregateFunctionDistinctSingleNumericImpl<T>>
{
public:

    AggregateFunctionDistinctSingleNumericImpl(AggregateFunctionPtr nested, const DataTypes & arguments)
        : AggregateFunctionDistinctBase<
            AggregateFunctionDistinctSingleNumericData<T>,
            AggregateFunctionDistinctSingleNumericImpl<T>>(nested, arguments) {}

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        if (this->data(place).value.insert(vec[row_num]).second)
            this->nested_func->add(this->getNestedPlace(place), columns, row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;

        auto arguments = this->argument_types[0]->createColumn();
        for (auto & elem : rhs_set)
            if (cur_set.insert(elem.getValue()).second)
                arguments->insert(elem.getValue());

        const auto * arguments_ptr = arguments.get();
        if (!arguments->empty())
            this->nested_func->addBatchSinglePlace(arguments->size(), this->getNestedPlace(place), &arguments_ptr, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).value.write(buf);
        this->nested_func->serialize(this->getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).value.read(buf);
        this->nested_func->deserialize(this->getNestedPlace(place), buf, arena);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        this->nested_func->insertResultInto(this->getNestedPlace(place), to);
    }
};

struct AggregateFunctionDistinctSingleGenericData
{
    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, 4>;
    Set value;
};

template <bool is_plain_column = false>
class AggregateFunctionDistinctSingleGenericImpl final
    : public AggregateFunctionDistinctBase<AggregateFunctionDistinctSingleGenericData,
        AggregateFunctionDistinctSingleGenericImpl<is_plain_column>>
{
public:
    using Data = AggregateFunctionDistinctSingleGenericData;

    AggregateFunctionDistinctSingleGenericImpl(AggregateFunctionPtr nested, const DataTypes & arguments)
        : AggregateFunctionDistinctBase<
            AggregateFunctionDistinctSingleGenericData,
            AggregateFunctionDistinctSingleGenericImpl<is_plain_column>>(nested, arguments) {}

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;

        Data::Set::LookupResult it;
        bool inserted;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);
        if (inserted)
            this->nested_func->add(this->getNestedPlace(place), columns, row_num, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;

        Data::Set::LookupResult it;
        bool inserted;
        auto arguments = this->argument_types[0]->createColumn();
        for (auto & elem : rhs_set)
        {
            cur_set.emplace(ArenaKeyHolder{elem.getValue(), *arena}, it, inserted);
            if (inserted)
                deserializeAndInsert<is_plain_column>(elem.getValue(), *arguments);
        }

        const auto * arguments_ptr = arguments.get();
        if (!arguments->empty())
            this->nested_func->addBatchSinglePlace(arguments->size(), this->getNestedPlace(place), &arguments_ptr, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        auto & set = this->data(place).value;
        writeVarUInt(set.size(), buf);
        for (const auto & elem : set)
            writeStringBinary(elem.getValue(), buf);

        this->nested_func->serialize(this->getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            set.insert(readStringBinaryInto(*arena, buf));

        this->nested_func->deserialize(this->getNestedPlace(place), buf, arena);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to) const override
    {
        this->nested_func->insertResultInto(this->getNestedPlace(place), to);
    }
};

}
