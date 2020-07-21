#pragma once

#include <cassert>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE 0xFFFFFF


namespace DB
{


template <typename T>
struct AggregateFunctionGroupUniqArrayData
{
    /// When creating, the hash table must be small.
    using Set = HashSetWithStackMemory<T, DefaultHash<T>, 4>;

    Set value;
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T, typename Tlimit_num_elem>
class AggregateFunctionGroupUniqArray
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>, AggregateFunctionGroupUniqArray<T, Tlimit_num_elem>>
{
    static constexpr bool limit_num_elems = Tlimit_num_elem::value;
    UInt64 max_elems;

private:
    using State = AggregateFunctionGroupUniqArrayData<T>;

public:
    AggregateFunctionGroupUniqArray(const DataTypePtr & argument_type, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayData<T>,
          AggregateFunctionGroupUniqArray<T, Tlimit_num_elem>>({argument_type}, {}),
          max_elems(max_elems_) {}

    String getName() const override { return "groupUniqArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>());
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if (limit_num_elems && this->data(place).value.size() >= max_elems)
            return;
        this->data(place).value.insert(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        if (!limit_num_elems)
            this->data(place).value.merge(this->data(rhs).value);
        else
        {
            auto & cur_set = this->data(place).value;
            auto & rhs_set = this->data(rhs).value;

            for (auto & rhs_elem : rhs_set)
            {
                if (cur_set.size() >= max_elems)
                    return;
                cur_set.insert(rhs_elem.getValue());
            }
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        auto & set = this->data(place).value;
        size_t size = set.size();
        writeVarUInt(size, buf);
        for (const auto & elem : set)
            writeIntBinary(elem, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).value.read(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        size_t size = set.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->getValue();
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupUniqArrayGenericData
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 3; /// adjustable

    using Set = HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash,
        INITIAL_SIZE_DEGREE>;

    Set value;
};

template <bool is_plain_column>
static void deserializeAndInsertImpl(StringRef str, IColumn & data_to);

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns groupUniqArray() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false, typename Tlimit_num_elem = std::false_type>
class AggregateFunctionGroupUniqArrayGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericData,
        AggregateFunctionGroupUniqArrayGeneric<is_plain_column, Tlimit_num_elem>>
{
    DataTypePtr & input_data_type;

    static constexpr bool limit_num_elems = Tlimit_num_elem::value;
    UInt64 max_elems;

    using State = AggregateFunctionGroupUniqArrayGenericData;

public:
    AggregateFunctionGroupUniqArrayGeneric(const DataTypePtr & input_data_type_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggregateFunctionGroupUniqArrayGenericData, AggregateFunctionGroupUniqArrayGeneric<is_plain_column, Tlimit_num_elem>>({input_data_type_}, {})
        , input_data_type(this->argument_types[0])
        , max_elems(max_elems_) {}

    String getName() const override { return "groupUniqArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(input_data_type);
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        auto & set = this->data(place).value;
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
        {
            writeStringBinary(elem.getValue(), buf);
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        size_t size;
        readVarUInt(size, buf);
        //TODO: set.reserve(size);

        for (size_t i = 0; i < size; ++i)
            set.insert(readStringBinaryInto(*arena, buf));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        if (limit_num_elems && set.size() >= max_elems)
            return;

        bool inserted;
        State::Set::LookupResult it;
        auto key_holder = getKeyHolder<is_plain_column>(*columns[0], row_num, *arena);
        set.emplace(key_holder, it, inserted);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;

        bool inserted;
        State::Set::LookupResult it;
        for (auto & rhs_elem : rhs_set)
        {
            if (limit_num_elems && cur_set.size() >= max_elems)
                return;

            // We have to copy the keys to our arena.
            assert(arena != nullptr);
            cur_set.emplace(ArenaKeyHolder{rhs_elem.getValue(), *arena}, it, inserted);
        }
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & set = this->data(place).value;
        offsets_to.push_back(offsets_to.back() + set.size());

        for (auto & elem : set)
            deserializeAndInsert<is_plain_column>(elem.getValue(), data_to);
    }
};

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE

}
