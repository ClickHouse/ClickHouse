#pragma once

#include <queue>
#include <type_traits>
#include "Common/HashTable/FixedHashMap.h"
#include "Common/HashTable/FixedHashSet.h"
#include "Common/HashTable/HashMap.h"
#include "Common/HashTable/HashSet.h"
#include "IO/ReadHelpers.h"
#include "IO/VarInt.h"
#include "IO/WriteHelpers.h"
#include "base/StringRef.h"
#include "base/types.h"

#define AGGREGATE_FUNCTION_GRAPH_MAX_SIZE 0xFFFFF

namespace DB 
{

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

// template<typename Key, typename Value>
// using NeededHashMap = std::conditional_t<is_arithmetic_v<Key>, FixedHashMap<Key, Value>, HashMap<Key, Value>>;
// template<typename Key>
// using NeededHashSet = std::conditional_t<is_arithmetic_v<Key>, FixedHashSet<Key>, HashSet<Key>>;

template<typename Key, typename Value>
using NeededHashMap = HashMap<Key, Value>;
template<typename Key>
using NeededHashSet = HashSet<Key>;

namespace impl 
{

template<typename DataType>
struct DirectionalGraphDataImpl 
{
    using Vertex = std::conditional_t<is_arithmetic_v<DataType>, DataType, StringRef>;
    using VertexSet = NeededHashSet<Vertex>;
    using VertexMap = NeededHashMap<Vertex, Vertex>;
    using GraphType = NeededHashMap<Vertex, std::vector<Vertex>>;
    GraphType graph{};
    size_t edges_count = 0;

    void merge(const DirectionalGraphDataImpl & rhs)
    {
        edges_count += rhs.edges_count;
        if (unlikely(edges_count > AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
            throw Exception("Too large graph size", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
        for (const auto & elem : rhs.graph)
        {
            auto & children = graph[elem.getKey()];
            children.insert(children.end(), elem.getMapped().begin(), elem.getMapped().end());
        }
    }

    bool isTree() const
    {
        if (graph.empty())
            return true;
        if (graph.size() != edges_count + 1)
            return false;
        VertexSet leafs;
        leafs.reserve(graph.size());
        for (const auto & [from, to] : graph)
            for (Vertex leaf : to)
                leafs.insert(leaf);
        if (edges_count != leafs.size())
            return false;
        Vertex root;
        for (const auto & [from, to] : graph)
        {
            if (!leafs.has(from))
            {
                root = from;
                break;
            }
        }
        VertexSet visited;
        visited.reserve(graph.size());
        visited.insert(root);
        std::queue<std::pair<Vertex, Vertex>> buffer{{{root, root}}};
        while (!buffer.empty())
        {
            auto [vertex, parent] = buffer.front();
            buffer.pop();
            for (const auto & to : graph.at(vertex))
            {
                typename VertexSet::LookupResult it;
                bool inserted;
                visited.emplace(to, it, inserted);
                if (!inserted)
                    return false;
                buffer.emplace(to, vertex);
            }
        }

        return visited.size() == graph.size();
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(graph.size(), buf);
        for (const auto & elem : graph)
        {
            writeBinary(elem.getKey(), buf);
            writeBinary(elem.getMapped(), buf);
        }
    }
};

}

template<typename DataType>
struct DirectionalGraphData;

template<>
struct DirectionalGraphData<StringRef> : impl::DirectionalGraphDataImpl<StringRef>
{
    void deserialize(ReadBuffer & buf, Arena * arena)
    {
        graph = {};
        edges_count = 0;
        size_t size;
        readBinary(size, buf);

        graph.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            Vertex key = readStringBinaryInto(*arena, buf);
            size_t children_count = 0;
            readBinary(children_count, buf);
            edges_count += children_count;
            if (unlikely(edges_count > AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
                throw Exception("Too large graph size to serialize", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
            auto & children = graph[key];
            children.reserve(children_count);
            for (size_t child_idx = 0; child_idx < children_count; ++child_idx)
                children.push_back(readStringBinaryInto(*arena, buf));
        }
    }

    void add(const IColumn ** columns, size_t row_num, Arena * arena)
    {
        if (unlikely(edges_count == AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
        {
            throw Exception("Too large graph size", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
        }
        const char * begin = nullptr;
        Vertex key = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
        Vertex value = columns[1]->serializeValueIntoArena(row_num, *arena, begin);
        graph[key].push_back(value);
        graph.insert({value, {}});
        ++edges_count;
    }

    static constexpr bool allocatesMemoryInArena()
    {
        return true;
    }
};

template<typename DataType>
struct DirectionalGraphData : impl::DirectionalGraphDataImpl<DataType>
{
    using Vertex = typename impl::DirectionalGraphDataImpl<DataType>::Vertex;
    using VertexSet = typename impl::DirectionalGraphDataImpl<DataType>::VertexSet;
    using VertexMap = typename impl::DirectionalGraphDataImpl<DataType>::VertexMap;
    using GraphType = typename impl::DirectionalGraphDataImpl<DataType>::GraphType;
    using impl::DirectionalGraphDataImpl<DataType>::graph;
    using impl::DirectionalGraphDataImpl<DataType>::edges_count;

    void deserialize(ReadBuffer & buf, Arena * )
    {
        graph = {};
        edges_count = 0;
        size_t size;
        readVarUInt(size, buf);

        graph.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            Vertex key;
            readBinary(key, buf);
            size_t children_count = 0;
            readBinary(children_count, buf);
            edges_count += children_count;
            if (unlikely(edges_count > AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
                throw Exception("Too large graph size to serialize", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
            auto & children = graph[key];
            children.reserve(children_count);
            for (size_t child_idx = 0; child_idx < children_count; ++child_idx) {
                children.emplace_back();
                readBinary(children.back(), buf);
            }
        }
    }

    void add(const IColumn ** columns, size_t row_num, Arena * )
    {
        if (unlikely(edges_count == AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
        {
            throw Exception("Too large graph size", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
        }
        Vertex key = assert_cast<const ColumnVector<Vertex> &>(*columns[0]).getData()[row_num];
        Vertex value = assert_cast<const ColumnVector<Vertex> &>(*columns[1]).getData()[row_num];
        graph[key].push_back(value);
        graph.insert({value, {}});
        ++edges_count;
    }

    static constexpr bool allocatesMemoryInArena()
    {
        return false;
    }
};

}
