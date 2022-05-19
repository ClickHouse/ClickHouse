#include "Common/HashTable/HashSet.h"
#include <Common/logger_useful.h>
#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphBidirectionalData.h"
#include "base/types.h"

namespace DB
{
template <typename VertexType>
class GraphCountBridges final : public GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBridges<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<BidirectionalGraphData<VertexType>, GraphCountBridges<VertexType>>)

    static constexpr const char * name = "GraphCountBridges";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    std::vector<Vertex> getDfsOrder(ConstAggregateDataPtr __restrict place, Vertex vertex, HashMap<Vertex, UInt64>& tin, HashMap<Vertex, Vertex>& parents, UInt64& timer) const {
        std::vector<Vertex> order;
        VertexSet used;
        std::vector<std::pair<Vertex, std::decay_t<decltype(data(place).graph.at(vertex).begin())>>> dfs_stack;
        dfs_stack.emplace_back(vertex, data(place).graph.at(vertex).begin());
        used.insert(vertex);
        parents[vertex] = vertex;
        tin[vertex] = timer++;
        while (!dfs_stack.empty()) {
            auto [vertex, it] = dfs_stack.back();
            dfs_stack.pop_back();
            if (it == data(place).graph.at(vertex).end()) {
                order.push_back(vertex);
            } else {
                auto cp_it = it;
                ++cp_it;
                dfs_stack.emplace_back(vertex, cp_it);
                if (!used.has(*it)) {
                    Vertex next = *it;
                    dfs_stack.emplace_back(next, data(place).graph.at(next).begin());
                    used.insert(next);
                    tin[next] = timer++;
                    parents[next] = vertex;
                }
            }
        }
        return order;
    }

    void countBridges(
        ConstAggregateDataPtr __restrict place,
        Vertex from,
        VertexSet & used,
        HashMap<Vertex, UInt64> & tin,
        HashMap<Vertex, UInt64> & up,
        UInt64 & cntBridges,
        UInt64 & timer) const
    {
        HashMap<Vertex, Vertex> parents;
        auto order = getDfsOrder(place, from, tin, parents, timer);

        for (Vertex vertex : order) {
            up[vertex] = tin[vertex];
        }
        
        std::string s;
        
        // if constexpr (std::is_same_v<Vertex, UInt64>) {
        //     s += "order = ";
        //     for (auto i : order) {
        //         s += std::to_string(i);
        //         s += " ";
        //     }
        //     s += "tin = ";
        //     for (auto i : order) {
        //         s += std::to_string(tin[i]);
        //         s += " ";
        //     }
        // }

        for (Vertex vertex : order) {
            // up[vertex] = tin[vertex];
            used.insert(vertex);
            Vertex parent = parents.at(vertex);
            HashMap<Vertex, UInt64> edges;
            for (Vertex next : data(place).graph.at(vertex)) {
                ++edges[next];
            }
            for (Vertex next : data(place).graph.at(vertex)) {
                if (next != parent) {
                    if (used.has(next)) {
                        up[vertex] = std::min(up[vertex], up[next]);
                        if (up[next] > tin[vertex] && edges.at(next) == 1) {
                            // if constexpr (std::is_same_v<Vertex, UInt64>) {
                            //     s += "vertex = " + std::to_string(vertex) + ", next = " + std::to_string(next) + " | ";
                            // }
                            ++cntBridges;
                        }
                    } else {
                        up[vertex] = std::min(up[vertex], tin[next]);
                    }
                }
            }
        }

        // if constexpr (std::is_same_v<Vertex, UInt64>) {
        //     s += "up = ";
        //     for (auto i : order) {
        //         s += std::to_string(up[i]);
        //         s += " ";
        //     }
        //     s += "\n";
        // }

        // std::cout << s << std::endl;

        // throw std::runtime_error(s);

        // used.insert(vertex);
        // tin[vertex] = timer;
        // up[vertex] = timer;
        // ++timer;

        // for (Vertex next : data(place).graph.at(vertex))
        // {
        //     if (next == parent)
        //         continue;
        //     else if (used.has(next))
        //         up[vertex] = std::min(up[vertex], tin[next]);
        //     else
        //     {
        //         countBridges(place, next, vertex, used, tin, up, cntBridges, timer);
        //         up[vertex] = std::min(up[vertex], up[next]);
        //         if (up[next] > tin[vertex])
        //             ++cntBridges;
        //     }
        // }
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena *) const
    {
        const auto & graph = data(place).graph;
        if (graph.size() < 2)
            return 0;
        VertexSet used;
        HashMap<Vertex, UInt64> tin, up;
        UInt64 cnt_bridges = 0;
        UInt64 timer = 0;
        countBridges(place, graph.begin()->getKey(), used, tin, up, cnt_bridges, timer);
        return cnt_bridges;
    }
};

INSTANTIATE_GRAPH_OPERATION(GraphCountBridges)

}
