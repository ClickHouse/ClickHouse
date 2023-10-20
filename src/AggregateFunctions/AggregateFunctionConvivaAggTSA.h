#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>

// Conviva timeline library
#include "compiler.h"
#include "executor.h"

#define AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE 0xFFFFFF


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

/// A particular case is an implementation for numeric types.
template <typename T>
struct ConvivaAggTSANumericData
{
    /// For easy serialization.
    static_assert(std::has_unique_object_representations_v<T> || std::is_floating_point_v<T>);

    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;
    Array value;
};

template <typename T>
class ConvivaAggTSANumericImpl final
    : public IAggregateFunctionDataHelper<ConvivaAggTSANumericData<T>, ConvivaAggTSANumericImpl<T>>
{
    using Data = ConvivaAggTSANumericData<T>;
    std::string yaml_config;
    Executor executor;
    UInt64 max_elems;

public:
    explicit ConvivaAggTSANumericImpl(
        const DataTypePtr & data_type_, const Array & parameters_, const std::string & yaml_config_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<ConvivaAggTSANumericData<T>, ConvivaAggTSANumericImpl<T>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , yaml_config(yaml_config_)
        , executor([&]() -> Executor {
                       Compiler compiler(yaml_config);
                       compiler.compile();
                       return Executor(compiler.getPhysicalPlan());
                   }())
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return "convivaAggTSA"; }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
        auto & cur_elems = this->data(place);  // reinterpret cast of the aggregation data, which is of type ConvivaAggTSANumericData

        if (cur_elems.value.size() >= max_elems)
        {
            return;
        }

        cur_elems.value.push_back(row_value, arena);

        // TODO: add TSA logic here
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        if (rhs_elems.value.empty())
            return;

        UInt64 elems_to_insert = std::min(static_cast<size_t>(max_elems) - cur_elems.value.size(), rhs_elems.value.size());
        if (elems_to_insert)
            cur_elems.value.insertByOffsets(rhs_elems.value, 0, elems_to_insert, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & value = this->data(place).value;
        const size_t size = value.size();
        writeVarUInt(size, buf);
        for (const auto & element : value)
            writeBinaryLittleEndian(element, buf);

        // TODO: handle DAG state
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE);

        if (unlikely(size > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = this->data(place).value;

        value.resize_exact(size, arena);
        for (auto & element : value)
            readBinaryLittleEndian(element, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        // This part is not touched, so far only events of the string type are handled
        const auto & value = this->data(place).value;
        size_t size = value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};


/// General case

/// Nodes used to implement a linked list for storage of convivaAggTSA states
template <typename Node>
struct ConvivaAggTSANodeBase
{
    UInt64 size; // size of payload

    /// Returns pointer to actual payload
    char * data() { return reinterpret_cast<char *>(this) + sizeof(Node); }

    const char * data() const { return reinterpret_cast<const char *>(this) + sizeof(Node); }

    /// Clones existing node (does not modify next field)
    Node * clone(Arena * arena) const
    {
        return reinterpret_cast<Node *>(
            const_cast<char *>(arena->alignedInsert(reinterpret_cast<const char *>(this), sizeof(Node) + size, alignof(Node))));
    }

    /// Write node to buffer
    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size, buf);
        buf.write(data(), size);
    }

    /// Reads and allocates node from ReadBuffer's data (doesn't set next)
    static Node * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);
        if (unlikely(size > AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.readStrict(node->data(), size);
        return node;
    }
};

struct ConvivaAggTSANodeString : public ConvivaAggTSANodeBase<ConvivaAggTSANodeString>
{
    using Node = ConvivaAggTSANodeString;

    /// Create node from string
    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef string = assert_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + string.size, alignof(Node)));
        node->size = string.size;
        memcpy(node->data(), string.data, string.size);

        return node;
    }

    void insertInto(IColumn & column)
    {
        assert_cast<ColumnString &>(column).insertData(data(), size);
    }

    std::string toString() const
    {
        return std::string(data(), size);
    }
};

struct ConvivaAggTSANodeGeneral : public ConvivaAggTSANodeBase<ConvivaAggTSANodeGeneral>
{
    using Node = ConvivaAggTSANodeGeneral;

    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        const char * begin = arena->alignedAlloc(sizeof(Node), alignof(Node));
        StringRef value = column.serializeValueIntoArena(row_num, *arena, begin);

        Node * node = reinterpret_cast<Node *>(const_cast<char *>(begin));
        node->size = value.size;

        return node;
    }

    void insertInto(IColumn & column) { column.deserializeAndInsertFromArena(data()); }

    std::string toString() const
    {
        return std::string(data(), size);
    }
};

template <typename Node>
struct ConvivaAggTSAGeneralData
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(Node *), 4096>;
    using Array = PODArray<Node *, 32, Allocator>;
    Array value;
};

/// Implementation of convivaAggTSA for String or any ComplexObject via Array
template <typename Node>
class ConvivaAggTSAGeneralImpl final
    : public IAggregateFunctionDataHelper<ConvivaAggTSAGeneralData<Node>, ConvivaAggTSAGeneralImpl<Node>>
{
    using Data = ConvivaAggTSAGeneralData<Node>;
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    std::string yaml_config;
    Executor executor;
    UInt64 max_elems;

public:
    ConvivaAggTSAGeneralImpl(const DataTypePtr & data_type_, const Array & parameters_, const std::string & yaml_config_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<ConvivaAggTSAGeneralData<Node>, ConvivaAggTSAGeneralImpl<Node>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , data_type(this->argument_types[0])
        , yaml_config(yaml_config_)
        , executor([&]() -> Executor {
                       Compiler compiler(yaml_config);
                       compiler.compile();
                       return Executor(compiler.getPhysicalPlan());
                   }())
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return "convivaAggTSA"; }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_elems = data(place);

        if (cur_elems.value.size() >= max_elems)
        {
            return;
        }

        Node * node = Node::allocate(*columns[0], row_num, arena);
        cur_elems.value.push_back(node, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = data(place);
        auto & rhs_elems = data(rhs);

        if (rhs_elems.value.empty())
            return;

        UInt64 new_elems;
        if (cur_elems.value.size() >= max_elems)
            return;
        new_elems = std::min(rhs_elems.value.size(), static_cast<size_t>(max_elems) - cur_elems.value.size());
        for (UInt64 i = 0; i < new_elems; ++i)
            cur_elems.value.push_back(rhs_elems.value[i]->clone(arena), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).value.size(), buf);

        auto & value = data(place).value;
        for (auto & node : value)
            node->write(buf);

        // TODO: handle DAG state
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        UInt64 elems;
        readVarUInt(elems, buf);

        if (unlikely(elems == 0))
            return;

        if (unlikely(elems > AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE);

        if (unlikely(elems > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = data(place).value;

        value.resize_exact(elems, arena);
        for (UInt64 i = 0; i < elems; ++i)
            value[i] = Node::read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column_array = assert_cast<ColumnArray &>(to);
        auto & column_data = column_array.getData();

        // So far only events of the string type are supported.
        if (std::is_same_v<Node, ConvivaAggTSANodeString>)
        {
            std::vector<std::string> json_events;
            // TODO: sort the input array

            auto & value = data(place).value;
            for (auto & node : value)
            {
                json_events.push_back(node->toString());
            }

            auto executor_ptr = const_cast<Executor*>(&executor);  // Hack because insertResultInto() is marked as const function
            executor_ptr->ingestStringEvents(json_events, "timestamp");
            auto result = executor_ptr->execute();

            // Clear the previous state of the merged column_array, because the events are moved into the executor.
            // Next round all parts' array of events start with empty state.
            auto * column_str = typeid_cast<ColumnString *>(&column_data);
            column_str->getChars().clear();
            column_str->getOffsets().clear();
            column_array.getOffsets().resize(0);

            // Insert the new result
            auto & offsets = column_array.getOffsets();
            offsets.push_back(1);  // Single element, so offset is 1
            column_data.insert(result[0]);
        }
        else
        {
            auto & offsets = column_array.getOffsets();
            offsets.push_back(offsets.back() + data(place).value.size());

            auto & value = data(place).value;
            for (auto & node : value)
                node->insertInto(column_data);
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};

#undef AGGREGATE_FUNCTION_CONVIVA_AGG_TSA_MAX_ARRAY_SIZE

}