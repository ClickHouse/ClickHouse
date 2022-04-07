#include "AggregateFunctionGraphOperation.h"
#include "AggregateFunctions/AggregateFunctionGraphDirectionalData.h"
#include "DataTypes/DataTypesNumber.h"
#include "base/types.h"
#include <DataTypes/DataTypeFactory.h>
#include "Common/HashTable/HashSet.h"

namespace DB
{

namespace ErrorCodes 
{
extern const int UNSUPPORTED_PARAMETER;
}

template<typename VertexType>
class TreeHeight final : public GraphOperation<DirectionalGraphData<VertexType>, TreeHeight<VertexType>>
{
public:
    INHERIT_GRAPH_OPERATION_USINGS(GraphOperation<DirectionalGraphData<VertexType>, TreeHeight<VertexType>>)

    static constexpr const char* name = "TreeHeight";

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeUInt64>(); }

    UInt64 calculateOperation(ConstAggregateDataPtr __restrict place, Arena*) const {
        if (data(place).edges_count == 0) {
            return 0;
        }
        const auto& graph = this->data(place).graph;
        Vertex root;
        VertexSet not_roots;
        for (const auto& [from, to] : graph) {
            for (Vertex vertex : to) {
                not_roots.insert(vertex);
            }
        }
        if (data(place).edges_count != not_roots.size()) {
            throw Exception("1Graph must have structure of tree", ErrorCodes::UNSUPPORTED_PARAMETER); 
        }
        for (const auto& [from, to] : graph) {
            if (not_roots.find(from) == not_roots.end()) {
                root = from;
                break;
            }
        }
        VertexSet visited;
        std::queue<std::pair<Vertex, UInt64>> buffer;
        buffer.emplace(root, 0);
        UInt64 result = 0;
        while (!buffer.empty()) {
            auto [vertex, distance] = buffer.front();
            buffer.pop();
            result = std::max(result, distance);
            typename VertexSet::LookupResult it;
            bool inserted;
            visited.emplace(vertex, it, inserted);
            if (!inserted) {
                throw Exception("2Graph must have structure of tree", ErrorCodes::UNSUPPORTED_PARAMETER); 
            }
            if (const auto* iter = graph.find(vertex)) {
                for (const auto& to : iter->getMapped()) {
                    buffer.emplace(to, distance + 1);
                }

            }
        }
        if (visited.size() != not_roots.size() + 1) {
            throw Exception("3Graph must have structure of tree", ErrorCodes::UNSUPPORTED_PARAMETER); 
        }

        return result;
    }
};

INSTANTIATE_GRAPH_OPERATION(TreeHeight)

}
