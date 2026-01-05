#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTime.h>

#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    template <typename DecimalType>
    DecimalField<DecimalType> extractFromNode(const Node * node, UInt32 default_scale)
    {
        constexpr bool result_is_timestamp = std::is_same_v<DecimalType, DateTime64>;
        constexpr std::string_view what = result_is_timestamp ? "timestamp" : "duration";

        switch (node->node_type)
        {
            case NodeType::Duration:
            {
                auto decimal64 = static_cast<const PQT::Duration *>(node)->duration;
                return DecimalField<DecimalType>{decimal64.getValue(), decimal64.getScale()};
            }
            case NodeType::ScalarLiteral:
            {
                auto scalar = static_cast<const PQT::ScalarLiteral *>(node)->scalar;
                auto scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(default_scale);
                return DecimalField<DecimalType>{static_cast<Int64>(scalar * scale_multiplier + 0.5), default_scale};
            }
            default:
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Cannot parse node of type {} as a {}",
                                node->node_type, what);
            }
        }
    }
}

DecimalField<DateTime64> nodeToTime(const Node * scalar_or_duration_node, UInt32 default_scale)
{
    return extractFromNode<DateTime64>(scalar_or_duration_node, default_scale);
}

DecimalField<Decimal64> nodeToDuration(const Node * scalar_or_duration_node, UInt32 default_scale)
{
    return extractFromNode<Decimal64>(scalar_or_duration_node, default_scale);
}

}
