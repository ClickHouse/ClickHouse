#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/SpaceSaving.h>

#include <Common/FieldVisitors.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{


// Allow NxK more space before calculating top K to increase accuracy
#define TOP_K_LOAD_FACTOR 3


template <typename T>
struct AggregateFunctionTopKData
{
    using Set = SpaceSaving
    <
        T,
        HashCRC32<T>,
        HashTableGrower<4>,
        HashTableAllocatorWithStackMemory<sizeof(T) * (1 << 4)>
    >;
    Set value;
};


template <typename T>
class AggregateFunctionTopK
    : public IAggregateFunctionDataHelper<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T>>
{
private:
    using State = AggregateFunctionTopKData<T>;

    UInt64 threshold;
    UInt64 reserved;

public:
    AggregateFunctionTopK(UInt64 threshold)
        : threshold(threshold), reserved(TOP_K_LOAD_FACTOR * threshold) {}

    String getName() const override { return "topK"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>());
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);
        set.insert(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).value.merge(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        auto & set = this->data(place).value;
        set.resize(reserved);
        set.read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const typename State::Set & set = this->data(place).value;
        auto result_vec = set.topK(threshold);
        size_t size = result_vec.size();

        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        typename ColumnVector<T>::Container & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = result_vec.begin(); it != result_vec.end(); ++it, ++i)
            data_to[old_size + i] = it->key;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionTopKGenericData
{
    using Set = SpaceSaving
    <
        StringRef,
        StringRefHash,
        HashTableGrower<4>,
        HashTableAllocatorWithStackMemory<sizeof(StringRef) * (1 << 4)>
    >;

    Set value;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns topK() can be implemented more efficently (especially for small numeric arrays).
 */
template <bool is_plain_column = false>
class AggregateFunctionTopKGeneric : public IAggregateFunctionDataHelper<AggregateFunctionTopKGenericData, AggregateFunctionTopKGeneric<is_plain_column>>
{
private:
    using State = AggregateFunctionTopKGenericData;

    UInt64 threshold;
    UInt64 reserved;
    DataTypePtr input_data_type;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    AggregateFunctionTopKGeneric(UInt64 threshold, const DataTypePtr & input_data_type)
        : threshold(threshold), reserved(TOP_K_LOAD_FACTOR * threshold), input_data_type(input_data_type) {}

    String getName() const override { return "topK"; }

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
        this->data(place).value.write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        set.clear();
        set.resize(reserved);

        // Specialized here because there's no deserialiser for StringRef
        size_t count = 0;
        readVarUInt(count, buf);
        for (size_t i = 0; i < count; ++i)
        {
            auto ref = readStringBinaryInto(*arena, buf);
            UInt64 count, error;
            readVarUInt(count, buf);
            readVarUInt(error, buf);
            set.insert(ref, count, error);
            arena->rollback(ref.size);
        }

        set.readAlphaMap(buf);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);

        if constexpr (is_plain_column)
        {
            set.insert(columns[0]->getDataAt(row_num));
        }
        else
        {
            const char * begin = nullptr;
            StringRef str_serialized = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
            set.insert(str_serialized);
            arena->rollback(str_serialized.size);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).value.merge(this->data(rhs).value);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto result_vec = this->data(place).value.topK(threshold);
        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + result_vec.size());

        for (auto & elem : result_vec)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.key.data, elem.key.size);
            else
                data_to.deserializeAndInsertFromArena(elem.key.data);
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


#undef TOP_K_LOAD_FACTOR

}
