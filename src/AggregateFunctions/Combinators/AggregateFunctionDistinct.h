#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashMap.h>
#include <IO/ReadHelpersArena.h>


namespace DB
{
struct Settings;


template <typename T>
struct AggregateFunctionDistinctSingleNumericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;
    using Self = AggregateFunctionDistinctSingleNumericData<T>;

    /// queue will hold values that are not yet processed. Once processed, the
    /// contents of the queue will be passed to history.
    mutable Set history;
    mutable Set queue;

    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena *)
    {
        const auto & vec = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();
        const T value = vec[row_num];

        if (!history.contains(value))
        {
            queue.insert(value);
        }
    }

    /// We make sure that the merged queue does not contain values that are
    /// already processed by rhs.
    void merge(const Self & rhs, Arena *)
    {
        std::vector<T> to_erase;
        to_erase.reserve(queue.size());

        for (auto it = queue.begin(); it != queue.end(); ++it)
        {
            const T v = it->getValue();
            if (rhs.history.contains(v))
                to_erase.push_back(v);
        }
        for (const T v : to_erase)
            queue.erase(v);

        history.merge(rhs.history);

        /// Make sure queue does not contain elements that exist in history
        for (const auto & elem : rhs.queue)
        {
            const T v = elem.getValue();
            if (!history.contains(v))
                queue.insert(v);
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        history.write(buf);
        queue.write(buf);
    }

    void deserialize(ReadBuffer & buf, Arena *)
    {
        history.read(buf);
        queue.read(buf);
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());

        /// Only sending the contents of the queue.
        for (const auto & elem : queue)
        {
            const auto value = elem.getValue();
            argument_columns[0]->insert(value);
            history.insert(value);
        }

        queue.~Set();
        new (&queue) Set{};
        return argument_columns;
    }
};

struct AggregateFunctionDistinctGenericData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, 4>;
    using Self = AggregateFunctionDistinctGenericData;

    mutable Set history;
    mutable Set queue;

    void merge(const Self & rhs, Arena * arena)
    {
        std::vector<StringRef> to_erase;
        to_erase.reserve(queue.size());

        for (auto it = queue.begin(); it != queue.end(); ++it)
        {
            const auto v = it->getValue();
            if (rhs.history.contains(v))
                to_erase.push_back(v);
        }

        for (const auto & v : to_erase)
            queue.erase(v);

        {
            Set::LookupResult it;
            bool inserted;
            for (const auto & elem : rhs.history)
                history.emplace(ArenaKeyHolder{elem.getValue(), *arena}, it, inserted);
        }

        {
            Set::LookupResult it;
            bool inserted;
            for (const auto & elem : rhs.queue)
            {
                const StringRef v = elem.getValue();
                if (!history.contains(v))
                    queue.emplace(ArenaKeyHolder{v, *arena}, it, inserted);
            }
        }
    }

    void serialize(WriteBuffer & buf) const
    {
        writeVarUInt(history.size(), buf);
        for (const auto & elem : history)
            writeStringBinary(elem.getValue(), buf);

        writeVarUInt(queue.size(), buf);
        for (const auto & elem : queue)
            writeStringBinary(elem.getValue(), buf);
    }

    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        size_t size;
        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            history.insert(readStringBinaryInto(*arena, buf));

        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            queue.insert(readStringBinaryInto(*arena, buf));
    }
};

template <bool is_plain_column>
struct AggregateFunctionDistinctSingleGenericData : public AggregateFunctionDistinctGenericData
{
    void add(const IColumn ** columns, size_t /* columns_num */, size_t row_num, Arena * arena)
    {
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);

        if (!history.contains(key_holder.key))
        {
            Set::LookupResult it;
            bool inserted;
            queue.emplace(key_holder, it, inserted);
        }
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns;
        argument_columns.emplace_back(argument_types[0]->createColumn());

        for (const auto & elem : queue)
        {
            const auto v = elem.getValue();
            deserializeAndInsert<is_plain_column>(v, *argument_columns[0]);
            history.insert(v);
        }

        queue.~Set();
        new (&queue) Set{};
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

        if (!history.contains(value))
        {
            Set::LookupResult it;
            bool inserted;
            auto key_holder = SerializedKeyHolder{value, *arena};
            queue.emplace(key_holder, it, inserted);
        }
    }

    MutableColumns getArguments(const DataTypes & argument_types) const
    {
        MutableColumns argument_columns(argument_types.size());
        for (size_t i = 0; i < argument_types.size(); ++i)
            argument_columns[i] = argument_types[i]->createColumn();

        for (const auto & elem : queue)
        {
            const char * begin = elem.getValue().data;
            for (auto & column : argument_columns)
                begin = column->deserializeAndInsertFromArena(begin);

            history.insert(elem.getValue());
        }

        queue.~Set();
        new (&queue) Set{};
        return argument_columns;
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
        this->data(place).add(columns, arguments_num, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).merge(this->data(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        this->data(place).deserialize(buf, arena);
    }

    template <bool MergeResult>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto arguments = this->data(place).getArguments(this->argument_types);
        ColumnRawPtrs arguments_raw(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            arguments_raw[i] = arguments[i].get();

        assert(!arguments.empty());
        nested_func->addBatchSinglePlace(0, arguments[0]->size(), getNestedPlace(place), arguments_raw.data(), arena);
        if constexpr (MergeResult)
            nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
        else
            nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
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
