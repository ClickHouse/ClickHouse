#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/SpaceSaving.h>

#include <Common/FieldVisitors.h>

#include <AggregateFunctions/AggregateFunctionGroupArray.h>


namespace DB
{


// Allow NxK more space before calculating top K to increase accuracy
#define TOP_K_DEFAULT 10
#define TOP_K_LOAD_FACTOR 3
#define TOP_K_MAX_SIZE 0xFFFFFF


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
    : public IUnaryAggregateFunction<AggregateFunctionTopKData<T>, AggregateFunctionTopK<T>>
{
private:
    using State = AggregateFunctionTopKData<T>;
    UInt64 threshold = TOP_K_DEFAULT;
    UInt64 reserved = TOP_K_LOAD_FACTOR * threshold;

public:
    String getName() const override { return "topK"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>());
    }

    void setArgument(const DataTypePtr & /*argument*/)
    {
    }

    void setParameters(const Array & params) override
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (k > TOP_K_MAX_SIZE)
            throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(TOP_K_MAX_SIZE),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (k == 0)
            throw Exception("Parameter 0 is illegal for aggregate function " + getName(),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = k;
        reserved = TOP_K_LOAD_FACTOR * k;
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
            set.resize(reserved);
        set.insert(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
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
class AggregateFunctionTopKGeneric : public IUnaryAggregateFunction<AggregateFunctionTopKGenericData, AggregateFunctionTopKGeneric<is_plain_column>>
{
private:
    using State = AggregateFunctionTopKGenericData;
    DataTypePtr input_data_type;
    UInt64 threshold = TOP_K_DEFAULT;
    UInt64 reserved = TOP_K_LOAD_FACTOR * threshold;

    static void deserializeAndInsert(StringRef str, IColumn & data_to);

public:
    String getName() const override { return "topK"; }

    void setArgument(const DataTypePtr & argument)
    {
        input_data_type = argument;
    }

    void setParameters(const Array & params) override
    {
        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires exactly one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        UInt64 k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (k > TOP_K_MAX_SIZE)
            throw Exception("Too large parameter for aggregate function " + getName() + ". Maximum: " + toString(TOP_K_MAX_SIZE),
                ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        threshold = k;
        reserved = TOP_K_LOAD_FACTOR * k;
    }

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

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        auto & set = this->data(place).value;
        if (set.capacity() != reserved)
        {
            set.resize(reserved);
        }

        StringRef str_serialized = column.getDataAt(row_num);
        set.insert(str_serialized);
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
            deserializeAndInsert(elem.key, data_to);
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


template <>
inline void AggregateFunctionTopKGeneric<false>::deserializeAndInsert(StringRef str, IColumn & data_to)
{
    data_to.deserializeAndInsertFromArena(str.data);
}

template <>
inline void AggregateFunctionTopKGeneric<true>::deserializeAndInsert(StringRef str, IColumn & data_to)
{
    data_to.insertData(str.data, str.size);
}


#undef TOP_K_DEFAULT
#undef TOP_K_MAX_SIZE
#undef TOP_K_LOAD_FACTOR

}
