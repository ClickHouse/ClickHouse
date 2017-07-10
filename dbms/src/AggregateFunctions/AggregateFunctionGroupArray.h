#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>

#include <Common/PODArrayArena.h>

#include <AggregateFunctions/IUnaryAggregateFunction.h>

#include <common/likely.h>
#include <type_traits>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}


/// A particular case is an implementation for numeric types.
template <typename T>
struct AggregateFunctionGroupArrayDataNumeric
{
    /// Memory is allocated to several elements immediately so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArray<T>);

    using Array = PODArray<T, bytes_in_arena, AllocatorWithStackMemory<Allocator<false>, bytes_in_arena>>;
    Array value;
};


template <typename T>
class AggregateFunctionGroupArrayNumeric final
    : public IUnaryAggregateFunction<AggregateFunctionGroupArrayDataNumeric<T>, AggregateFunctionGroupArrayNumeric<T>>
{
public:
    String getName() const override { return "groupArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>());
    }

    void setArgument(const DataTypePtr & argument)
    {
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        this->data(place).value.push_back(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).value.insert(this->data(rhs).value.begin(), this->data(rhs).value.end());
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(&value[0]), size * sizeof(value[0]));
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        auto & value = this->data(place).value;

        value.resize(size);
        buf.read(reinterpret_cast<char *>(&value[0]), size * sizeof(value[0]));
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
    }
};


/// A particular case is an implementation for numeric types.
template <typename T>
struct AggregateFunctionGroupArrayDataNumeric2
{
    /// Memory is allocated to several elements immediately so that the state occupies 64 bytes.
    static constexpr size_t bytes_in_arena = 64 - sizeof(PODArrayArenaAllocator<T>);

    using Array = PODArrayArenaAllocator<T, bytes_in_arena, ArenaAllocatorWithStackMemoty<bytes_in_arena>>;
    Array value;
};


template <typename T, typename Tlimit_num_elems>
class AggregateFunctionGroupArrayNumeric2 final
    : public IUnaryAggregateFunction<AggregateFunctionGroupArrayDataNumeric2<T>, AggregateFunctionGroupArrayNumeric2<T, Tlimit_num_elems>>
{
    static constexpr bool limit_num_elems = Tlimit_num_elems::value;
    UInt64 max_elems;

public:
    AggregateFunctionGroupArrayNumeric2(UInt64 max_elems_ = std::numeric_limits<UInt64>::max()) : max_elems(max_elems_) {}

    String getName() const override { return "groupArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<T>>());
    }

    void setArgument(const DataTypePtr & argument) {}

    void setParameters(const Array & params) override
    {
        if (!limit_num_elems && !params.empty())
            throw Exception("This instatintion of " + getName() + "aggregate function doesn't accept any parameters. It is a bug.", ErrorCodes::LOGICAL_ERROR);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena * arena) const
    {
        if (limit_num_elems && this->data(place).value.size() >= max_elems)
            return;

        this->data(place).value.push_back(static_cast<const ColumnVector<T> &>(column).getData()[row_num], arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        if (!limit_num_elems)
        {
            cur_elems.value.insert(rhs_elems.value.begin(), rhs_elems.value.end(), arena);
        }
        else
        {
            UInt64 elems_to_insert = std::min(max_elems - cur_elems.value.size(), rhs_elems.value.size());
            cur_elems.value.insert(rhs_elems.value.begin(), rhs_elems.value.begin() + elems_to_insert, arena);
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(&value[0]), size * sizeof(value[0]));
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        if (limit_num_elems && unlikely(size > max_elems))
            throw Exception("Too large array size, it should not exceed " + toString(max_elems), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        auto & value = this->data(place).value;

        value.resize(size, arena);
        buf.read(reinterpret_cast<char *>(&value[0]), size * sizeof(value[0]));
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets_t & offsets_to = arr_to.getOffsets();

        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        typename ColumnVector<T>::Container_t & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
    }
};


/// General case (inefficient). NOTE You can also implement a special case for strings.
struct AggregateFunctionGroupArrayDataGeneric
{
    Array value;    /// TODO Add MemoryTracker
};


/// Puts all values to an array, general case. Implemented inefficiently.
class AggregateFunctionGroupArrayGeneric final
    : public IUnaryAggregateFunction<AggregateFunctionGroupArrayDataGeneric, AggregateFunctionGroupArrayGeneric>
{
private:
    DataTypePtr type;

public:
    String getName() const override { return "groupArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgument(const DataTypePtr & argument)
    {
        type = argument;
    }


    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        data(place).value.push_back(Array::value_type());
        column.get(row_num, data(place).value.back());
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).value.insert(data(place).value.end(), data(rhs).value.begin(), data(rhs).value.end());
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const Array & value = data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
            type->serializeBinary(value[i], buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        Array & value = data(place).value;

        value.resize(size);
        for (size_t i = 0; i < size; ++i)
            type->deserializeBinary(value[i], buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        to.insert(data(place).value);
    }
};


namespace
{

struct NodeString;
struct NodeGeneral;

template <typename Node>
struct NodeBase
{
    Node * next;
    UInt64 size;

    char * data()
    {
        static_assert(sizeof(NodeBase) == sizeof(Node));
        return reinterpret_cast<char *>(this) + sizeof(Node);
    }

    /// Clones existing node (does not modify next field)
    Node * clone(Arena * arena)
    {
        return reinterpret_cast<Node *>(const_cast<char *>(arena->insert(reinterpret_cast<char *>(this), sizeof(Node) + size)));
    }

    /// Write node to buffer
    void write(WriteBuffer & buf)
    {
        writeVarUInt(size, buf);
        buf.write(data(), size);
    }

    /// Reads and allocates node from ReadBuffer's data (doesn't set next)
    static Node * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);

        Node * node = reinterpret_cast<Node *>(arena->alloc(sizeof(Node) + size));
        node->size = size;
        buf.read(node->data(), size);
        return node;
    }
};

struct NodeString : public NodeBase<NodeString>
{
    using Node = NodeString;

    /// Create node from string
    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef string = static_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alloc(sizeof(Node) + string.size));
        node->next = nullptr;
        node->size = string.size;
        memcpy(node->data(), string.data, string.size);

        return node;
    }

    void insertInto(IColumn & column)
    {
        static_cast<ColumnString &>(column).insertData(data(), size);
    }
};


struct NodeGeneral : public NodeBase<NodeGeneral>
{
    using Node = NodeGeneral;

    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        const char * begin = arena->alloc(sizeof(Node));
        StringRef value = column.serializeValueIntoArena(row_num, *arena, begin);

        Node * node = reinterpret_cast<Node *>(const_cast<char *>(begin));
        node->next = nullptr;
        node->size = value.size;

        return node;
    }

    void insertInto(IColumn & column)
    {
        column.deserializeAndInsertFromArena(data());
    }
};


template <typename Node>
struct AggregateFunctionGroupArrayListImpl_Data
{
    UInt64 elems = 0;
    Node * first = nullptr;
    Node * last = nullptr;
};


/// Implementation of groupArray(String or ComplexObject) via linked list
/// It has poor performance in case of many small objects
template <typename Node, bool limit_num_elems>
class AggregateFunctionGroupArrayStringListImpl final
    : public IUnaryAggregateFunction<AggregateFunctionGroupArrayListImpl_Data<Node>, AggregateFunctionGroupArrayStringListImpl<Node, limit_num_elems>>
{
    using Data = AggregateFunctionGroupArrayListImpl_Data<Node>;
    static Data & data(AggregateDataPtr place)            { return *reinterpret_cast<Data*>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data*>(place); }

    DataTypePtr data_type;
    UInt64 max_elems;

public:
    AggregateFunctionGroupArrayStringListImpl(UInt64 max_elems_ = std::numeric_limits<UInt64>::max()) : max_elems(max_elems_) {}

    String getName() const override { return "groupArray2"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(data_type); }

    void setParameters(const Array & params) override
    {
        if (!limit_num_elems && !params.empty())
            throw Exception("This instatintion of " + getName() + "aggregate function doesn't accept any parameters. It is a bug.", ErrorCodes::LOGICAL_ERROR);
    }

    void setArgument(const DataTypePtr & argument)
    {
        data_type = argument;
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena * arena) const
    {
        if (limit_num_elems && data(place).elems >= max_elems)
            return;

        Node * node = Node::allocate(column, row_num, arena);

        if (unlikely(!data(place).first))
        {
            data(place).first = node;
            data(place).last = node;
        }
        else
        {
            data(place).last->next = node;
            data(place).last = node;
        }

        ++data(place).elems;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        /// It is sadly, but rhs's Arena could be destroyed

        if (!data(rhs).first) /// rhs state is empty
            return;

        UInt64 new_elems;
        UInt64 cur_elems = data(place).elems;
        if (limit_num_elems)
        {
            if (data(place).elems >= max_elems)
                return;

            new_elems = std::min(data(place).elems + data(rhs).elems, max_elems);
        }
        else
        {
            new_elems = data(place).elems + data(rhs).elems;
        }

        Node * p_rhs = data(rhs).first;
        Node * p_lhs;

        if (unlikely(!data(place).last)) /// lhs state is empty
        {
            p_lhs = p_rhs->clone(arena);
            data(place).first = data(place).last = p_lhs;
            p_rhs = p_rhs->next;
            ++cur_elems;
        }
        else
        {
            p_lhs = data(place).last;
        }

        for (; cur_elems < new_elems; ++cur_elems)
        {
            Node * p_new = p_rhs->clone(arena);
            p_lhs->next = p_new;
            p_rhs = p_rhs->next;
            p_lhs = p_new;
        }

        p_lhs->next = nullptr;
        data(place).last = p_lhs;
        data(place).elems = new_elems;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).elems, buf);

        Node * p = data(place).first;
        while (p)
        {
            p->write(buf);
            p = p->next;
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        UInt64 elems;
        readVarUInt(elems, buf);
        data(place).elems = elems;

        if (unlikely(elems == 0))
            return;

        if (unlikely(elems > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        if (limit_num_elems && unlikely(elems > max_elems))
            throw Exception("Too large array size, it should not exceed " + toString(max_elems), ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        Node * prev = Node::read(buf, arena);
        data(place).first = prev;

        for (UInt64 i = 1; i < elems; ++i)
        {
            Node * cur = Node::read(buf, arena);
            prev->next = cur;
            prev = cur;
        }

        prev->next = nullptr;
        data(place).last = prev;
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & column_array = static_cast<ColumnArray &>(to);

        auto & offsets = column_array.getOffsets();
        offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + data(place).elems);

        auto & column_data = column_array.getData();

        if (std::is_same<Node, NodeString>::value)
        {
            auto & string_offsets = static_cast<ColumnString &>(column_data).getOffsets();
            string_offsets.reserve(string_offsets.size() + data(place).elems);
        }

        Node * p = data(place).first;
        while (p)
        {
            p->insertInto(column_data);
            p = p->next;
        }
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }
};

}


struct AggregateFunctionGroupArrayStringConcatImpl_Data
{
    static constexpr size_t target_size = 64;
    static constexpr size_t free_space = target_size - sizeof(PODArrayArenaAllocator<char>) - sizeof(PODArrayArenaAllocator<UInt64>);

    PODArrayArenaAllocator<char, 64> chars;
    PODArrayArenaAllocator<UInt64, free_space, ArenaAllocatorWithStackMemoty<free_space>> offsets;
};


class AggregateFunctionGroupArrayStringConcatImpl final
    : public IUnaryAggregateFunction<AggregateFunctionGroupArrayStringConcatImpl_Data, AggregateFunctionGroupArrayStringConcatImpl>
{
public:

    String getName() const override { return "groupArray4"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()); }

    void setArgument(const DataTypePtr & argument) {}

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena * arena) const
    {
        StringRef string = static_cast<const ColumnString &>(column).getDataAtWithTerminatingZero(row_num);

        data(place).chars.insert(string.data, string.data + string.size, arena);
        data(place).offsets.push_back(string.size, arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_state = data(place);
        auto & rhs_state = data(rhs);

        cur_state.chars.insert(rhs_state.chars.begin(), rhs_state.chars.end(), arena);
        cur_state.offsets.insert(rhs_state.offsets.begin(), rhs_state.offsets.end(), arena);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & column_array = static_cast<ColumnArray &>(to);
        auto & column_string = static_cast<ColumnString &>(column_array.getData());
        auto & offsets = column_array.getOffsets();
        auto & cur_state = data(place);

        offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + cur_state.offsets.size());

        auto pos = column_string.getChars().size();
        column_string.getChars().insert(cur_state.chars.begin(), cur_state.chars.end());

        column_string.getOffsets().reserve(column_string.getOffsets().size() + cur_state.offsets.size());
        for (UInt64 i = 0; i < cur_state.offsets.size(); ++i)
        {
            pos += cur_state.offsets[i];
            column_string.getOffsets().push_back(pos);
        }
    }
};


struct AggregateFunctionGroupArrayGeneric_SerializedData
{
    PODArrayArenaAllocator<StringRef> values;
};

class AggregateFunctionGroupArrayGeneric_SerializedDataImpl final
    : public IUnaryAggregateFunction<AggregateFunctionGroupArrayGeneric_SerializedData, AggregateFunctionGroupArrayGeneric_SerializedDataImpl>
{
private:
    DataTypePtr type;

public:
    String getName() const override { return "groupArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgument(const DataTypePtr & argument)
    {
        type = argument;
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena * arena) const
    {
        const char * begin = nullptr;
        data(place).values.push_back(column.serializeValueIntoArena(row_num, *arena, begin), arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        for (const StringRef & elem : data(rhs).values)
            data(place).values.push_back(StringRef(arena->insert(elem.data, elem.size), elem.size), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & column_array = typeid_cast<ColumnArray &>(to);
        auto & column_data = column_array.getData();
        auto & offsets = column_array.getOffsets();
        auto & cur_state = data(place);

        offsets.push_back((offsets.size() == 0 ? 0 : offsets.back()) + cur_state.values.size());

        column_data.reserve(cur_state.values.size());

        for (const StringRef & elem : cur_state.values)
            column_data.deserializeAndInsertFromArena(elem.data);
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }
};




struct AggregateFunctionGroupArrayGeneric_ColumnPtrImpl_Data
{
    ColumnPtr container;
};

class AggregateFunctionGroupArrayGeneric_ColumnPtrImpl final
    : public IUnaryAggregateFunction<AggregateFunctionGroupArrayGeneric_ColumnPtrImpl_Data, AggregateFunctionGroupArrayGeneric_ColumnPtrImpl>
{
private:
    DataTypePtr type;

public:
    String getName() const override { return "groupArray4"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgument(const DataTypePtr & argument)
    {
        type = argument;
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data;
        data(place).container = type->createColumn();
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        data(place).container->insertFrom(column, row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).container->insertRangeFrom(*data(rhs).container, 0, data(rhs).container->size());
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        UInt64 s = data(place).container->size();
        writeVarUInt(s, buf);
        type->serializeBinaryBulk(*data(place).container, buf, 0, s);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        UInt64 s;
        readVarUInt(s, buf);
        type->deserializeBinaryBulk(*data(place).container, buf, s, 0);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & array_column = typeid_cast<ColumnArray &>(to);
        auto s = data(place).container->size();
        array_column.getOffsets().push_back(s);
        array_column.getData().insertRangeFrom(*data(place).container, 0, s);
    }
};



#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
