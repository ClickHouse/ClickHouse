#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashSet.h>

#include <AggregateFunctions/IAggregateFunction.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE 0xFFFFFF


namespace DB
{


template <typename T>
struct AggregateFunctionGroupUniqArrayData
{
    /// When creating, the hash table must be small.
    using Set = HashSet<
        T,
        DefaultHash<T>,
        HashTableGrower<4>,
        HashTableAllocatorWithStackMemory<sizeof(T) * (1 << 4)>
    >;

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
        this->data(place).value.insert(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
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
        for (auto & elem : set)
            writeIntBinary(elem, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).value.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        size_t size = set.size();

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->getValue();
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggreagteFunctionGroupUniqArrayGenericData
{
    static constexpr size_t INIT_ELEMS = 2; /// adjustable
    static constexpr size_t ELEM_SIZE = sizeof(HashSetCellWithSavedHash<StringRef, StringRefHash>);
    using Set = HashSetWithSavedHash<StringRef, StringRefHash, HashTableGrower<INIT_ELEMS>, HashTableAllocatorWithStackMemory<INIT_ELEMS * ELEM_SIZE>>;

    Set value;
};


/// Helper function for deserialize and insert for the class AggreagteFunctionGroupUniqArrayGeneric
template <bool is_plain_column>
static StringRef getSerializationImpl(const IColumn & column, size_t row_num, Arena & arena);

template <bool is_plain_column>
static void deserializeAndInsertImpl(StringRef str, IColumn & data_to);

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns groupUniqArray() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false, typename Tlimit_num_elem = std::false_type>
class AggreagteFunctionGroupUniqArrayGeneric
    : public IAggregateFunctionDataHelper<AggreagteFunctionGroupUniqArrayGenericData, AggreagteFunctionGroupUniqArrayGeneric<is_plain_column, Tlimit_num_elem>>
{
    DataTypePtr & input_data_type;

    static constexpr bool limit_num_elems = Tlimit_num_elem::value;
    UInt64 max_elems;

    using State = AggreagteFunctionGroupUniqArrayGenericData;

    static StringRef getSerialization(const IColumn & column, size_t row_num, Arena & arena)
    {
        return getSerializationImpl<is_plain_column>(column, row_num, arena);
    }

    static void deserializeAndInsert(StringRef str, IColumn & data_to)
    {
        return deserializeAndInsertImpl<is_plain_column>(str, data_to);
    }

public:
    AggreagteFunctionGroupUniqArrayGeneric(const DataTypePtr & input_data_type, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<AggreagteFunctionGroupUniqArrayGenericData, AggreagteFunctionGroupUniqArrayGeneric<is_plain_column, Tlimit_num_elem>>({input_data_type}, {})
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
        {
            set.insert(readStringBinaryInto(*arena, buf));
        }
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;

        bool inserted;
        State::Set::iterator it;

        if (limit_num_elems && set.size() >= max_elems)
            return;
        StringRef str_serialized = getSerialization(*columns[0], row_num, *arena);

        set.emplace(str_serialized, it, inserted);

        if constexpr (!is_plain_column)
        {
            if (!inserted)
                arena->rollback(str_serialized.size);
        }
        else
        {
            if (inserted)
                it->getValueMutable().data = arena->insert(str_serialized.data, str_serialized.size);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_set = this->data(place).value;
        auto & rhs_set = this->data(rhs).value;

        bool inserted;
        State::Set::iterator it;
        for (auto & rhs_elem : rhs_set)
        {
            if (limit_num_elems && cur_set.size() >= max_elems)
                return ;
            cur_set.emplace(rhs_elem.getValue(), it, inserted);
            if (inserted)
            {
                if (it->getValue().size)
                    it->getValueMutable().data = arena->insert(it->getValue().data, it->getValue().size);
                else
                    it->getValueMutable().data = nullptr;
            }
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & set = this->data(place).value;
        offsets_to.push_back(offsets_to.back() + set.size());

        for (auto & elem : set)
        {
            deserializeAndInsert(elem.getValue(), data_to);
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


template <>
inline StringRef getSerializationImpl<false>(const IColumn & column, size_t row_num, Arena & arena)
{
    const char * begin = nullptr;
    return column.serializeValueIntoArena(row_num, arena, begin);
}

template <>
inline StringRef getSerializationImpl<true>(const IColumn & column, size_t row_num, Arena &)
{
    return column.getDataAt(row_num);
}

template <>
inline void deserializeAndInsertImpl<false>(StringRef str, IColumn & data_to)
{
    data_to.deserializeAndInsertFromArena(str.data);
}

template <>
inline void deserializeAndInsertImpl<true>(StringRef str, IColumn & data_to)
{
    data_to.insertData(str.data, str.size);
}

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_UNIQ_MAX_SIZE

}
