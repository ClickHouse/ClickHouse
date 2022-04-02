#include "AggregateFunctionGraphOperation.h"
#include <optional>
#include "Common/HashTable/HashSet.h"
#include <Common/HashTable/HashMap.h>
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

void DirectionalGraphGenericData::merge(const DirectionalGraphGenericData & rhs)
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

void DirectionalGraphGenericData::serialize(WriteBuffer & buf) const
{
    writeVarUInt(graph.size(), buf);
    for (const auto & elem : graph)
    {
        writeStringBinary(elem.getKey(), buf);
        writeVarUInt(elem.getMapped().size(), buf);
        for (StringRef child : elem.getMapped())
            writeStringBinary(child, buf);
    }
}

void DirectionalGraphGenericData::deserialize(ReadBuffer & buf, Arena * arena)
{
    graph = {};
    edges_count = 0;
    size_t size;
    readVarUInt(size, buf);

    graph.reserve(size);
    for (size_t i = 0; i < size; ++i)
    {
        StringRef key = readStringBinaryInto(*arena, buf);
        size_t children_count = 0;
        readVarUInt(children_count, buf);
        edges_count += children_count;
        if (unlikely(edges_count > AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
            throw Exception("Too large graph size to serialize", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
        auto & children = graph[key];
        children.reserve(children_count);
        for (size_t child_idx = 0; child_idx < children_count; ++child_idx)
            children.push_back(readStringBinaryInto(*arena, buf));
    }
}

void DirectionalGraphGenericData::add(const IColumn ** columns, size_t row_num, Arena * arena)
{
    if (unlikely(edges_count == AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
    {
        throw Exception("Too large graph size", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
    const char * begin = nullptr;
    StringRef key = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
    StringRef value = columns[1]->serializeValueIntoArena(row_num, *arena, begin);
    graph[key].push_back(value);
    graph.insert({value, {}});
    ++edges_count;
}

bool DirectionalGraphGenericData::isTree() const
{
    if (graph.empty())
        return true;
    HashSet<StringRef> leafs;
    leafs.reserve(graph.size());
    for (const auto & [from, to] : graph)
        for (StringRef leaf : to)
            leafs.insert(leaf);
    if (edges_count != leafs.size())
        return false;
    StringRef root;
    for (const auto & [from, to] : graph)
    {
        if (!leafs.has(from))
        {
            root = from;
            break;
        }
    }
    HashSet<StringRef> visited;
    visited.reserve(graph.size());
    visited.insert(root);
    std::queue<std::pair<StringRef, StringRef>> buffer{{{root, root}}};
    while (!buffer.empty())
    {
        auto [vertex, parent] = buffer.front();
        buffer.pop();
        for (const auto & to : graph.at(vertex))
        {
            HashSet<StringRef>::LookupResult it;
            bool inserted;
            visited.emplace(to, it, inserted);
            if (!inserted)
                return false;
            buffer.emplace(to, vertex);
        }
    }

    return visited.size() == graph.size();
}

void BidirectionalGraphGenericData::add(const IColumn ** columns, size_t row_num, Arena * arena)
{
    if (unlikely(edges_count == AGGREGATE_FUNCTION_GRAPH_MAX_SIZE))
    {
        throw Exception("Too large graph size", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
    const char * begin = nullptr;
    StringRef key = columns[0]->serializeValueIntoArena(row_num, *arena, begin);
    StringRef value = columns[1]->serializeValueIntoArena(row_num, *arena, begin);
    graph[key].push_back(value);
    graph[value].push_back(key);
    ++edges_count;
}

bool BidirectionalGraphGenericData::isTree() const
{
    if (graph.empty())
        return true;
    HashSet<StringRef> visited;
    visited.reserve(graph.size());
    visited.insert(graph.begin()->getKey());
    std::queue<std::pair<StringRef, StringRef>> buffer{{{graph.begin()->getKey(), graph.begin()->getKey()}}};
    while (!buffer.empty())
    {
        auto [vertex, parent] = buffer.front();
        buffer.pop();
        for (const auto & to : graph.at(vertex))
        {
            if (to == parent)
                continue;
            HashSet<StringRef>::LookupResult it;
            bool inserted;
            visited.emplace(to, it, inserted);
            if (!inserted)
                return false;
            buffer.emplace(to, vertex);
        }
    }

    return true;
}

size_t BidirectionalGraphGenericData::componentsCount() const
{
    size_t components_count = 0;
    HashSet<StringRef> visited;
    for (const auto & [from, tos] : graph)
    {
        HashSet<StringRef>::LookupResult it;
        bool inserted;
        visited.emplace(from, it, inserted);
        if (inserted)
        {
            ++components_count;
            std::deque<StringRef> buffer(tos.begin(), tos.end());
            while (!buffer.empty())
            {
                StringRef vertex = buffer.front();
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

} // namespace DB
