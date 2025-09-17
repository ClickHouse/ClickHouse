#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/ReadHelpersArena.h>
#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashSet.h>
#include <Common/assert_cast.h>


namespace DB
{
struct Settings;


template <typename T>
struct AggregateFunctionDistinctSingleNumericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;

    /// history will hold all values added so far
    Set history;

    /// Returns true if the value did not exist in the history before
    bool add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        const T value = vec[row_num];
        return history.insert(value).second;
    }

    /// Pass the new values from rhs to the nested function via argument_columns
    void merge(const Self & rhs, MutableColumns & argument_columns, Arena *)
    {
        for (const auto & elem : rhs.history)
        {
            const auto & value = elem.getValue();
            bool inserted = history.insert(value).second;

            if (inserted)
                argument_columns[0]->insert(value);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        history.write(buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        history.read(buf);
    }
};

struct AggregateFunctionDistinctGenericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;

    Set history;

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(history.size(), buf);
        for (const auto & elem : history)
            writeStringBinary(elem.getValue(), buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            history.insert(readStringBinaryInto(*arena, buf));
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericData : public AggregateFunctionDistinctGenericData
{
    bool add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena * arena)
    {
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        Set::LookupResult it;
        bool inserted;
        history.emplace(key_holder, it, inserted);

        return inserted;
    }

    void merge(const Self & rhs, MutableColumns & argument_columns, Arena * arena)
    {
        for (const auto & elem : rhs.history)
        {
            const auto & value = elem.getValue();
            Set::LookupResult it;
            bool inserted;
            history.emplace(ArenaKeyHolder{value, *arena}, it, inserted);

            if (inserted)
                deserializeAndInsert<is_plain_column>(it->getValue(), *argument_columns[0]);
        }
    }
};

struct AggregateFunctionDistinctMultipleGenericData : public AggregateFunctionDistinctGenericData
{
    bool add(const IColumn ** columns, size_t columns_num, size_t row_num, Arena * arena)
    {
        const char * begin = nullptr;
        StringRef value(begin, 0);
        for (size_t i = 0; i < columns_num; ++i)
        {
            auto cur_ref = columns[i]->serializeAggregationStateValueIntoArena(row_num, *arena, begin);
            value.data = cur_ref.data - value.size;
            value.size += cur_ref.size;
        }

        Set::LookupResult it;
        bool inserted;
        history.emplace(SerializedKeyHolder{value, *arena}, it, inserted);

        return inserted;
    }

    void merge(const Self & rhs, MutableColumns & argument_columns, Arena * arena)
    {
        for (const auto & elem : rhs.history)
        {
            const auto & value = elem.getValue();
            if (!history.contains(value))
            {
                Set::LookupResult it;
                bool inserted;
                history.emplace(ArenaKeyHolder{value, *arena}, it, inserted);
                const char * pos = it->getValue().data;
                for (auto & column : argument_columns)
                    pos = column->deserializeAndInsertAggregationStateValueFromArena(pos);
            }
        }
    }
};

/** Adaptor for aggregate functions.
  * Adding -Distinct suffix to aggregate function
**/
template <typename Data>
class AggregateFunctionDistinct final : public IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct<Data>>
{
private:
    AggregateFunctionPtr nested_func;
    size_t prefix_size;
    size_t arguments_num;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    MutableColumns prepareArgumentColumns() const
    {
        MutableColumns argument_columns;
        argument_columns.reserve(this->argument_types.size());
        for (const auto & type : this->argument_types)
            argument_columns.emplace_back(type->createColumn());

        return argument_columns;
    }

    void addToNested(size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena) const
    {
        nested_func->addBatchSinglePlace(row_begin, row_end, getNestedPlace(place), columns, arena);
    }

    void addToNested(AggregateDataPtr __restrict place, const MutableColumns & argument_columns, Arena * arena) const
    {
        ColumnRawPtrs arguments_raw(argument_columns.size());
        for (size_t i = 0; i < argument_columns.size(); ++i)
            arguments_raw[i] = argument_columns[i].get();

        assert(!argument_columns.empty());
        addToNested(0, argument_columns[0]->size(), place, arguments_raw.data(), arena);
    }

public:
    AggregateFunctionDistinct(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
    : IAggregateFunctionDataHelper<Data, AggregateFunctionDistinct>(arguments, params_, nested_func_->getResultType())
    , nested_func(nested_func_)
    , arguments_num(arguments.size())
    {
        size_t nested_size = nested_func->alignOfData();
        prefix_size = (sizeof(Data) + nested_size - 1) / nested_size * nested_size;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        bool added = this->data(place).add(columns, arguments_num, row_num, arena);
        if (added)
            addToNested(row_num, row_num + 1, place, columns, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto argument_columns = prepareArgumentColumns();
        this->data(place).merge(this->data(rhs), argument_columns, arena);
        addToNested(place, argument_columns, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return std::max(alignof(Data), nested_func->alignOfData());
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

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data> && nested_func->hasTrivialDestructor();
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    String getName() const override
    {
        return nested_func->getName() + "Distinct";
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_func->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
