#include "AggregateFunctionGraphOperation.h"
#include "base/types.h"
#include "Common/HashTable/HashSet.h"
#include <optional>

namespace DB
{

namespace ErrorCodes 
{
extern const int UNSUPPORTED_PARAMETER;
}


class GraphCountBipartiteMaximumMatching final : public GraphOperationGeneral<BidirectionalGraphGenericData, GraphCountBipartiteMaximumMatching>
{
public:
    using GraphOperationGeneral<BidirectionalGraphGenericData, GraphCountBipartiteMaximumMatching>::GraphOperationGeneral;

    static constexpr const char* name = "countBipartiteMaximumMatching";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    bool isBipartite(const HashMap<StringRef, std::vector<StringRef>>& graph, StringRef vertex, HashMap<StringRef, bool>& color, bool currentColor = true) const {
        color[vertex] = currentColor;
        for (StringRef next : graph.at(vertex)) {
            if (color.find(next) == color.end()) {
                if (!isBipartite(graph, next, color, true ^ currentColor)) {
                    return false;
                }
            } else if (color[next] == currentColor) {
                return false;
            }
        }
        return true;
    }

    std::optional<HashMap<StringRef, bool>> getColor(const HashMap<StringRef, std::vector<StringRef>>& graph) const {
        HashMap<StringRef, bool> color;
        for (const auto& [vertex, neighbours] : graph) {
            if (color.find(vertex) == color.end()) {
                if (!isBipartite(graph, vertex, color)) {
                    return std::nullopt;
                }
            }
        }
        return std::make_optional(std::move(color));
    }

    bool dfsMatch(StringRef vertex, UInt64 currentColor, const HashMap<StringRef, std::vector<StringRef>>& graph, HashMap<StringRef, UInt64>& used, HashMap<StringRef, StringRef>& matching) const {
        if (used[vertex] == currentColor) {
            return false;
        }
        used[vertex] = currentColor;
        for (StringRef next : graph.at(vertex)) {
            if (!matching.has(next)) {
                matching[next] = vertex;
                return true;
            }
        }
        for (StringRef next : graph.at(vertex)) {
            if (dfsMatch(matching[next], currentColor, graph, used, matching)) {
                matching[next] = vertex;
                return true;
            }
        }
        return false;
    }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena*) const {
        const auto& graph = this->data(place).graph;
        if (graph.empty()) {
            return 0;
        }
        const auto color = getColor(graph);
        if (color == std::nullopt) {
            throw Exception("Graph must be bipartite", ErrorCodes::UNSUPPORTED_PARAMETER); 
        }
        HashMap<StringRef, UInt64> used;
        HashMap<StringRef, StringRef> matching;
        UInt64 current_color = 0;
        UInt64 matching_size = 0;
        for (const auto& [vertex, neighbours] : graph) {
            if (color->at(vertex)) {
                if (dfsMatch(vertex, ++current_color, graph, used, matching)) {
                    ++matching_size;
                }
            }
        }
        return matching_size;
    }

};

template void registerGraphAggregateFunction<GraphCountBipartiteMaximumMatching>(AggregateFunctionFactory & factory);

}
