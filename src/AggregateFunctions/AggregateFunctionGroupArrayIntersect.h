#pragma once

#include <cassert>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>

#include <Core/Field.h>



namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}
struct Settings;


template <typename T>
struct AggregateFunctionGroupArrayIntersectData
{
    using Map = HashMap<T, UInt64>;
    Map value;
    UInt64 version = 0;
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T>
class AggregateFunctionGroupArrayIntersect
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>, AggregateFunctionGroupArrayIntersect<T>>
{

private:
    using State = AggregateFunctionGroupArrayIntersectData<T>;

public:
    AggregateFunctionGroupArrayIntersect(const DataTypePtr & argument_type, const Array & parameters_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
          AggregateFunctionGroupArrayIntersect<T>>({argument_type}, parameters_, std::make_shared<DataTypeArray>(argument_type)) {}

    AggregateFunctionGroupArrayIntersect(const DataTypePtr & argument_type, const Array & parameters_, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
          AggregateFunctionGroupArrayIntersect<T>>({argument_type}, parameters_, result_type_) {}

    String getName() const override { return "GroupArrayIntersect"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & version = this->data(place).version;
        ++version;
        const auto arr = assert_cast<const ColumnArray &>(*columns[0])[row_num].get<Array>();
        auto & map = this->data(place).value;
        if (version == 1)
        {
            
            for (const auto & elem: arr) {
                map[static_cast<T>(elem.get<T>())] = version;
            }
        }
        else
        {
            for (const auto & elem: arr) {
                auto value = map.find(static_cast<T>(elem.get<T>()));
                if (value != nullptr)
                    ++value->getMapped();
            }

        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        
        auto & map = this->data(place).value;
        auto & rhs_map = this->data(rhs).value;
        UInt64 version = this->data(place).version++;
        UInt64 rhs_version = this->data(rhs).version;
        for (auto & rhs_elem : rhs_map)
        {   
            if (rhs_elem.getMapped() != rhs_version)
                continue;
            auto value = map.find(rhs_elem.getKey());
            if (value != nullptr && value->getMapped() == version)
                ++value->getMapped();
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & map = this->data(place).value;
        auto & version = this->data(place).version;

        writeVarUInt(version, buf);
        writeVarUInt(map.size(), buf);

        for (const auto & elem : map)
        {
            writeIntBinary(elem.getKey(), buf);
            writeVarUInt(elem.getMapped(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        
        readVarUInt(this->data(place).version, buf);
        this->data(place).value.read(buf);
      
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & map = this->data(place).value;
        const auto & version = this->data(place).version;
        size_t size = 0;
        for (auto it = map.begin(); it != map.end(); ++it)
            if (version == it->getMapped())
                ++size;

        offsets_to.push_back(offsets_to.back() + size);

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + size);

        size_t i = 0;
        for (auto it = map.begin(); it != map.end(); ++it) 
        {
            if (version == it->getMapped())
                data_to[old_size + i++] = it->getKey();
        }
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupArrayIntersectGenericData
{
    static constexpr size_t INITIAL_SIZE_DEGREE = 4; /// adjustable

    using Map = HashMapWithStackMemory<StringRef, UInt64, StringRefHash,
        INITIAL_SIZE_DEGREE>;

    Map value;
    UInt64 version = 0;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns GroupArrayIntersect() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false>
class AggregateFunctionGroupArrayIntersectGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectGenericData,
        AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>
{
    DataTypePtr & input_data_type;


    using State = AggregateFunctionGroupArrayIntersectGenericData;

public:
    AggregateFunctionGroupArrayIntersectGeneric(const DataTypePtr & input_data_type_, const Array & parameters_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectGenericData, AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>({input_data_type_}, parameters_, std::make_shared<DataTypeArray>(input_data_type_))
        , input_data_type(this->argument_types[0]) {}

    String getName() const override { return "GroupArrayIntersect"; }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & map = this->data(place).value;
        auto & version = this->data(place).version;
        writeVarUInt(version, buf);
        writeVarUInt(map.size(), buf);

        for (const auto & elem : map)
        {
            writeStringBinary(elem.getKey(), buf);
            writeVarUInt(elem.getMapped(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & map = this->data(place).value;
        auto & version = this->data(place).version;
        size_t size;
        readVarUInt(version, buf);
        readVarUInt(size, buf);
        map.reserve(size);
        UInt64 elem_version;
        for (size_t i = 0; i < size; ++i) {
            auto key = readStringBinaryInto(*arena, buf);
            readVarUInt(elem_version, buf);
            if (elem_version == version)
                map[key] = version;
        }
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & map = this->data(place).value;
        auto & version = this->data(place).version;
        bool inserted;
        State::Map::LookupResult it;
        ++version;
        
        
        const auto arr = assert_cast<const ColumnArray &>(*columns[0])[row_num].get<Array &>();
        if (version == 1)
        {
            for (auto & elem: arr) {
                
                if constexpr (is_plain_column)
                {
                    map.emplace(ArenaKeyHolder{toString(elem), *arena}, it, inserted);
                }
                else
                {
                    //map.emplace(ArenaKeyHolder{toString(elem), arena}, it, inserted);
                    //const char * begin = nullptr;
                    //StringRef serialized = *columns[0].serializeValueIntoArena(row_num, *arena, begin);
                    //St
                    //assert(serialized.data != nullptr);
                    //map.emplace(SerializedKeyHolder{serialized, arena}, it, inserted);
                    map.emplace(ArenaKeyHolder{toString(elem), *arena}, it, inserted);
                }
                if (inserted)
                    new (&it->getMapped()) UInt64(version);
            }
            //throw Exception(ErrorCodes::BAD_ARGUMENTS, "kekmek {}", map.size());
        }
        else {
            for (auto & elem: arr) {
                auto value = map.find(toString(elem));
                if (value != nullptr)
                    ++value->getMapped();
            }
        }
        //throw Exception(ErrorCodes::BAD_ARGUMENTS, "kekmek2 {} version{}", map.size(), version);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        
        auto & map = this->data(place).value;
        auto & rhs_map = this->data(rhs).value;
        UInt64 version = this->data(place).version++;
        UInt64 rhs_version = this->data(rhs).version;
        for (auto & rhs_elem : rhs_map)
        {   
            if (rhs_elem.getMapped() != rhs_version)
                continue;
            auto value = map.find(rhs_elem.getKey());
            if (value != nullptr && value->getMapped() == version)
                ++value->getMapped();
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & map = this->data(place).value;
        auto & version = this->data(place).version;
        size_t size = 0;
        for (auto & elem : map)
            if (elem.getMapped() == version)
                size++;
        offsets_to.push_back(offsets_to.back() + size);
    
        for (auto & elem : map)
        {
            if (elem.getMapped() == version)
            { 
                
                if constexpr (is_plain_column)
                {
                    data_to.insertData(elem.getKey().data, elem.getKey().size);
                }
                else
                {
                    data_to.insertData(elem.getKey().data, elem.getKey().size);
                    //data_to.insert(Field(String(elem.getKey())));
                }
               // deserializeAndInsert<is_plain_column>(elem.getKey(), data_to);
            }
        }
    }
};

}
