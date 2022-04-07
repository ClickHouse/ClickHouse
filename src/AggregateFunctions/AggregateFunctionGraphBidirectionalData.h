#pragma once

#include "AggregateFunctionGraphDirectionalData.h"
#include "base/extended_types.h"

namespace DB
{

template<typename DataType>
struct BidirectionalGraphData : DirectionalGraphData<DataType>
{
    using Vertex = typename DirectionalGraphData<DataType>::Vertex;
    using VertexSet = typename DirectionalGraphData<DataType>::VertexSet;
    using VertexMap = typename DirectionalGraphData<DataType>::VertexMap;
    using GraphType = typename DirectionalGraphData<DataType>::GraphType;
    using DirectionalGraphData<DataType>::graph;
    using DirectionalGraphData<DataType>::edges_count;

    void add(const IColumn ** columns, size_t row_num, Arena * arena)
    {
        if (unlikely(edges_count == AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
            throw Exception("Too large graph size", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
        Vertex key;
        Vertex value;
        if constexpr (is_arithmetic_v<Vertex>) {
            key = assert_cast<const ColumnVector<Vertex> &>(*columns[0]).getData()[row_num];
            value = assert_cast<const ColumnVector<Vertex> &>(*columns[1]).getData()[row_num];
        } else {
            const char * begin = nullptr;
            key = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
            value = columns[1]->serializeValueIntoArena(row_num, *arena, begin);
        }
        graph[key].push_back(value);
        graph[value].push_back(key);
        ++edges_count;
    }

    bool isTree() const
    {
        if (graph.empty())
            return true;
        if (graph.size() != edges_count + 1)
            return false;
        VertexSet visited;
        visited.reserve(graph.size());
        visited.insert(graph.begin()->getKey());
        std::queue<std::pair<Vertex, Vertex>> buffer{{{graph.begin()->getKey(), graph.begin()->getKey()}}};
        while (!buffer.empty())
        {
            auto [vertex, parent] = buffer.front();
            buffer.pop();
            for (const auto & to : graph.at(vertex))
            {
                if (to == parent)
                    continue;
                typename VertexSet::LookupResult it;
                bool inserted;
                visited.emplace(to, it, inserted);
                if (!inserted)
                    return false;
                buffer.emplace(to, vertex);
            }
        }

        return true;
    }

    size_t componentsCount() const {
        size_t components_count = 0;
        VertexSet visited;
        for (const auto & [from, tos] : graph)
        {
            typename VertexSet::LookupResult it;
            bool inserted;
            visited.emplace(from, it, inserted);
            if (inserted)
            {
                ++components_count;
                std::deque<Vertex> buffer(tos.begin(), tos.end());
                while (!buffer.empty())
                {
                    Vertex vertex = buffer.front();
                    buffer.pop_front();
                    visited.emplace(vertex, it, inserted);
                    if (inserted)
                        for (const auto & to : graph.at(vertex))
                            buffer.push_back(to);
                }
            }
        }
        return components_count;
    }

    size_t componentSize(Vertex root, VertexSet* visited = nullptr) const {
        VertexSet visited_buffer;
        if (visited == nullptr) {
            visited = &visited_buffer;
        }
        visited->insert(root);
        size_t component_size = 0;
        std::queue<std::pair<Vertex, Vertex>> buffer{{{root, root}}};
        while (!buffer.empty())
        {
            auto [vertex, parent] = buffer.front();
            buffer.pop();
            ++component_size;
            for (const auto & to : graph.at(vertex))
                if (to != parent)
                {
                    typename VertexSet::LookupResult it;
                    bool inserted;
                    visited->emplace(to, it, inserted);
                    if (inserted)
                        buffer.emplace(to, vertex);
                }
        }
        return component_size;
    }
};


}
