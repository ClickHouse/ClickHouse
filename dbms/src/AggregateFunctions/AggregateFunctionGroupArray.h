#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>

#include <Common/ArenaAllocator.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <common/likely.h>
#include <type_traits>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int LOGICAL_ERROR;
}


/// A particular case is an implementation for numeric types.
template <typename T>
struct GroupArrayNumericData
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedArenaAllocator<4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;
};


template <typename T, typename Tlimit_num_elems>
class GroupArrayNumericImpl final
    : public IAggregateFunctionDataHelper<GroupArrayNumericData<T>, GroupArrayNumericImpl<T, Tlimit_num_elems>>
{
    static constexpr bool limit_num_elems = Tlimit_num_elems::value;
    DataTypePtr data_type;
    UInt64 max_elems;

public:
    explicit GroupArrayNumericImpl(const DataTypePtr & data_type_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : data_type(data_type_), max_elems(max_elems_) {}

    String getName() const override { return "groupArray"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(data_type);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (limit_num_elems && this->data(place).value.size() >= max_elems)
            return;

        this->data(place).value.push_back(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num], arena);
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
            UInt64 elems_to_insert = std::min(static_cast<size_t>(max_elems) - cur_elems.value.size(), rhs_elems.value.size());
            cur_elems.value.insert(rhs_elems.value.begin(), rhs_elems.value.begin() + elems_to_insert, arena);
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);
        buf.write(reinterpret_cast<const char *>(value.data()), size * sizeof(value[0]));
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
        buf.read(reinterpret_cast<char *>(value.data()), size * sizeof(value[0]));
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        const auto & value = this->data(place).value;
        size_t size = value.size();

        ColumnArray & arr_to = static_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back((offsets_to.size() == 0 ? 0 : offsets_to.back()) + size);

        typename ColumnVector<T>::Container & data_to = static_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/// General case


/// Nodes used to implement a linked list for storage of groupArray states

template <typename Node>
struct GroupArrayListNodeBase
{
    Node * next;
    UInt64 size; // size of payload

    /// Returns pointer to actual payload
    char * data()
    {
        static_assert(sizeof(GroupArrayListNodeBase) == sizeof(Node));
        return reinterpret_cast<char *>(this) + sizeof(Node);
    }

    /// Clones existing node (does not modify next field)
    Node * clone(Arena * arena)
    {
        return reinterpret_cast<Node *>(const_cast<char *>(arena->alignedInsert(reinterpret_cast<char *>(this), sizeof(Node) + size, alignof(Node))));
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

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.read(node->data(), size);
        return node;
    }
};

struct GroupArrayListNodeString : public GroupArrayListNodeBase<GroupArrayListNodeString>
{
    using Node = GroupArrayListNodeString;

    /// Create node from string
    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef string = static_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + string.size, alignof(Node)));
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

struct GroupArrayListNodeGeneral : public GroupArrayListNodeBase<GroupArrayListNodeGeneral>
{
    using Node = GroupArrayListNodeGeneral;

    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        const char * begin = arena->alignedAlloc(sizeof(Node), alignof(Node));
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
struct GroupArrayGeneralListData
{
    UInt64 elems = 0;
    Node * first = nullptr;
    Node * last = nullptr;
};


/// Implementation of groupArray for String or any ComplexObject via linked list
/// It has poor performance in case of many small objects
template <typename Node, bool limit_num_elems>
class GroupArrayGeneralListImpl final
    : public IAggregateFunctionDataHelper<GroupArrayGeneralListData<Node>, GroupArrayGeneralListImpl<Node, limit_num_elems>>
{
    using Data = GroupArrayGeneralListData<Node>;
    static Data & data(AggregateDataPtr place)            { return *reinterpret_cast<Data*>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data*>(place); }

    DataTypePtr data_type;
    UInt64 max_elems;

public:
    GroupArrayGeneralListImpl(const DataTypePtr & data_type, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : data_type(data_type), max_elems(max_elems_) {}

    String getName() const override { return "groupArray"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(data_type); }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (limit_num_elems && data(place).elems >= max_elems)
            return;

        Node * node = Node::allocate(*columns[0], row_num, arena);

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

        if (std::is_same_v<Node, GroupArrayListNodeString>)
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

    const char * getHeaderFilePath() const override { return __FILE__; }
};

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
