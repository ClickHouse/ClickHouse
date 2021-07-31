#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Columns/ColumnArray.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/AggregationCommon.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>


namespace DB
{
struct Settings;


template <typename T>
struct AggregateFunctionDistinctSingleNumericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;
    Set set;

    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        set.insert(vec[row_num]);
    }

    void merge(const Self & rhs, Arena *)
    {
        set.merge(rhs.set);
    }

    void serialize(WriteBuffer & buf) const
    {
        set.write(buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        set.read(buf);
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());
        for (const auto & elem : set)
            argument_columns[0]->insert(elem.getValue());

        return argument_columns;
    }
};

struct AggregateFunctionDistinctGenericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;
    Set set;

    void merge(const Self & rhs, Arena * arena)
    {
        Set::LookupResult it;
        bool inserted;
        for (const auto & elem : rhs.set)
            set.emplace(ArenaKeyHolder{elem.getValue(), *arena}, it, inserted);
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(set.size(), buf);
        for (const auto & elem : set)
            writeStringBinary(elem.getValue(), buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            set.insert(readStringBinaryInto(*arena, buf));
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericData : public AggregateFunctionDistinctGenericData
{
    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena * arena)
    {
        Set::LookupResult it;
        bool inserted;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());
        for (const auto & elem : set)
            deserializeAndInsert<is_plain_column>(elem.getValue(), *argument_columns[0]);

        return argument_columns;
    }
};

struct AggregateFunctionDistinctMultipleGenericData : public AggregateFunctionDistinctGenericData
{
    void add(const IColumn ** columns, size_t columns_num, size_t row_num, Arena * arena)
    {
        const char * begin = nullptr;
        StringRef value(begin, 0);
        for (size_t i = 0; i < columns_num; ++i)
        {
            auto cur_ref = columns[i]->serializeValueIntoArena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        Set::LookupResult it;
        bool inserted;
        auto key_holder = SerializedKeyHolder{value, *arena};
        set.emplace(key_holder, it, inserted);
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        for (const auto & elem : set)
        {
            const char * begin = elem.getValue().data;
            for (auto & column : argument_columns)
                begin = column->deserializeAndInsertFromArena(begin);
        }

        return argument_columns;
    }
};

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/
template <typename Data>
class AggregateFunctionDistinct : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct<Data>>
{
private:
    static constexpr auto prefix_size = sizeof(Data);
    AggregateFunctionPtr nested_func;
    size_t arguments_num;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
    : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct>(arguments, params_)
    , nested_func(nested_func_)
    , arguments_num(arguments.size()) {}

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto arguments = this->data(place).getArguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        assert(!arguments.empty());
        nested_func->addBatchSinglePlace(arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena);
        nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
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

    bool isState() const override
    {
        return nested_func->isState();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
