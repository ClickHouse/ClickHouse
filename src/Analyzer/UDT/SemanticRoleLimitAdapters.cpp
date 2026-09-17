#include <Analyzer/UDT/SemanticRoleLimitAdapters.h>

#include <algorithm>

namespace DB::UDT
{
namespace
{

UInt64 lower(UInt64 component_maximum, const EffectiveResourceLimits & limits, ResourceLimit limit) noexcept
{
    return std::min(component_maximum, limits.get(limit));
}

}

SemanticRolePlannerLimits makeSemanticRolePlannerLimits(const EffectiveResourceLimits & limits) noexcept
{
    SemanticRolePlannerLimits result;
    result.maximum_sinks = lower(result.maximum_sinks, limits, ResourceLimit::SemanticSinksPerQuery);
    result.maximum_demanded_states = lower(result.maximum_demanded_states, limits, ResourceLimit::NodePathStatesPerQuery);
    result.maximum_inspected_edges = lower(result.maximum_inspected_edges, limits, ResourceLimit::InspectedEdgesPerQuery);
    result.maximum_logical_paths = lower(result.maximum_logical_paths, limits, ResourceLimit::NodePathStatesPerQuery);
    result.maximum_shapes = lower(result.maximum_shapes, limits, ResourceLimit::InternedLogicalShapesPerQuery);
    result.maximum_roles = lower(result.maximum_roles, limits, ResourceLimit::DenseRoleRecordsPerQuery);
    result.maximum_shape_bytes = lower(result.maximum_shape_bytes, limits, ResourceLimit::SemanticScratchBytesPerQuery);
    result.maximum_role_argument_bytes = lower(result.maximum_role_argument_bytes, limits, ResourceLimit::SemanticScratchBytesPerQuery);
    result.maximum_single_role_argument_bytes
        = lower(result.maximum_single_role_argument_bytes, limits, ResourceLimit::CanonicalActualArgumentBytes);
    result.maximum_combined_scratch_bytes
        = lower(result.maximum_combined_scratch_bytes, limits, ResourceLimit::SemanticScratchBytesPerQuery);
    result.maximum_literal_bytes = lower(result.maximum_literal_bytes, limits, ResourceLimit::ContextualLiteralBytesPerQuery);
    result.maximum_conflicts = lower(result.maximum_conflicts, limits, ResourceLimit::ConflictSamplesPerQuery);
    return result;
}

QueryTreeSemanticRoleGraphLimits makeQueryTreeSemanticRoleGraphLimits(const EffectiveResourceLimits & limits) noexcept
{
    QueryTreeSemanticRoleGraphLimits result;
    result.maximum_semantic_sinks = lower(result.maximum_semantic_sinks, limits, ResourceLimit::SemanticSinksPerQuery);
    return result;
}

}
