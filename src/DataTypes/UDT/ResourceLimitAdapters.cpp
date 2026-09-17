#include <DataTypes/UDT/ResourceLimitAdapters.h>

#include <algorithm>
#include <array>
#include <limits>

namespace DB::UDT
{
namespace
{

UInt64 lower(UInt64 component_maximum, const EffectiveResourceLimits & limits, ResourceLimit limit) noexcept
{
    return std::min(component_maximum, limits.get(limit));
}

UInt64 checkedProduct(UInt64 lhs, UInt64 rhs, std::string_view description)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        throw ResourceLimitError(ResourceLimitError::Code::ExceedsImplementationMaximum, description);
    return lhs * rhs;
}

UInt64 checkedSum(UInt64 lhs, UInt64 rhs, std::string_view description)
{
    if (rhs > std::numeric_limits<UInt64>::max() - lhs)
        throw ResourceLimitError(ResourceLimitError::Code::ExceedsImplementationMaximum, description);
    return lhs + rhs;
}
}

void lowerPersistedTypeReferencesLimits(PersistedTypeReferencesLimits & result, const EffectiveResourceLimits & limits) noexcept
{
    result.maximum_occurrence_paths = lower(result.maximum_occurrence_paths, limits, ResourceLimit::OccurrencePathsPerObject);
    result.maximum_sidecar_bytes = lower(result.maximum_sidecar_bytes, limits, ResourceLimit::SidecarBytesPerObject);
}

ResourceLimitLayer makeAuthorityResourceLimitLayer(const TypeAuthorityLimits & limits)
{
    ResourceLimitLayer result(ResourceLimitLayerKind::AuthorityAdapter);
    result.set(ResourceLimit::DefinitionsPerDatabase, limits.maximum_definitions)
        .set(ResourceLimit::LogicalTemplateNodes, limits.maximum_template_nodes)
        .set(ResourceLimit::DirectDependencies, limits.maximum_direct_dependencies)
        .set(ResourceLimit::TransitiveDependencies, limits.maximum_transitive_dependencies)
        .set(ResourceLimit::CheckerExpansionWorkUnits, limits.maximum_checker_work);
    return result;
}

EffectiveResourceLimits makeDefaultQueryEffectiveResourceLimits(const TypeAuthorityLimits & authority_limits)
{
    const std::array<ResourceLimitLayer, 3> layers{
        makeDatabaseDefaultResourceLimitLayer(),
        makeQueryDefaultResourceLimitLayer(),
        makeAuthorityResourceLimitLayer(authority_limits),
    };
    return calculateEffectiveResourceLimits(layers);
}

EffectiveResourceLimits
makeQueryEffectiveResourceLimits(const EffectiveResourceLimits & effective_database_limits, const TypeAuthorityLimits & authority_limits)
{
    const std::array<ResourceLimitLayer, 2> query_layers{
        makeQueryDefaultResourceLimitLayer(),
        makeAuthorityResourceLimitLayer(authority_limits),
    };
    return minimumEffectiveResourceLimits(effective_database_limits, calculateEffectiveResourceLimits(query_layers));
}

TemplateCheckerLimits makeTemplateCheckerLimits(const EffectiveResourceLimits & limits)
{
    TemplateCheckerLimits result;
    const UInt64 definitions = limits.get(ResourceLimit::DefinitionsPerDatabase);
    const UInt64 logical_nodes = limits.get(ResourceLimit::LogicalTemplateNodes);
    const UInt64 direct_dependencies = limits.get(ResourceLimit::DirectDependencies);
    const UInt64 transitive_dependencies = limits.get(ResourceLimit::TransitiveDependencies);
    const UInt64 definition_work = limits.get(ResourceLimit::CheckerExpansionWorkUnits);
    const UInt64 catalog_bytes = limits.get(ResourceLimit::DeterministicCatalogBytesPerDatabase);
    const UInt64 dependency_edges
        = checkedProduct(definitions, direct_dependencies, "TemplateChecker aggregate dependency-edge domain overflows UInt64");
    const UInt64 closure_work_per_definition = checkedProduct(
        direct_dependencies,
        checkedSum(transitive_dependencies, 1, "TemplateChecker closure-width domain overflows UInt64"),
        "TemplateChecker per-definition closure-work domain overflows UInt64");

    result.maximum_definitions = definitions;
    result.maximum_catalog_input_bytes = catalog_bytes;
    result.maximum_canonical_catalog_bytes = catalog_bytes;
    result.maximum_catalog_nodes = checkedProduct(definitions, logical_nodes, "TemplateChecker aggregate node domain overflows UInt64");
    result.maximum_catalog_edges = dependency_edges;
    result.maximum_catalog_checker_work = checkedSum(
        checkedSum(
            checkedProduct(definitions, definition_work, "TemplateChecker aggregate definition-work domain overflows UInt64"),
            checkedProduct(definitions, closure_work_per_definition, "TemplateChecker aggregate closure-work domain overflows UInt64"),
            "TemplateChecker aggregate definition and closure work domain overflows UInt64"),
        checkedSum(
            checkedProduct(3, definitions, "TemplateChecker aggregate graph-node work domain overflows UInt64"),
            checkedProduct(3, dependency_edges, "TemplateChecker aggregate graph-edge work domain overflows UInt64"),
            "TemplateChecker aggregate graph work domain overflows UInt64"),
        "TemplateChecker aggregate work domain overflows UInt64");
    result.maximum_scratch_bytes = std::min(
        template_checker_implementation_maximum_scratch_bytes,
        checkedProduct(4, catalog_bytes, "TemplateChecker aggregate scratch-byte domain overflows UInt64"));

    if (result.maximum_catalog_nodes > template_checker_implementation_maximum_catalog_nodes
        || result.maximum_catalog_edges > template_checker_implementation_maximum_catalog_edges
        || result.maximum_catalog_checker_work > template_checker_implementation_maximum_catalog_work)
    {
        throw ResourceLimitError(
            ResourceLimitError::Code::ExceedsImplementationMaximum,
            "TemplateChecker aggregate resource domain exceeds its implementation maximum");
    }
    result.maximum_formals = limits.get(ResourceLimit::FormalParameters);
    result.maximum_template_nodes = limits.get(ResourceLimit::LogicalTemplateNodes);
    result.maximum_logical_node_occurrences = limits.get(ResourceLimit::LogicalTemplateNodes);
    result.maximum_template_depth = limits.get(ResourceLimit::TemplateDepth);
    result.maximum_direct_dependencies = limits.get(ResourceLimit::DirectDependencies);
    result.maximum_transitive_dependencies = limits.get(ResourceLimit::TransitiveDependencies);
    result.maximum_checker_work = limits.get(ResourceLimit::CheckerExpansionWorkUnits);
    result.maximum_canonical_definition_bytes = limits.get(ResourceLimit::CanonicalDefinitionBytes);
    return result;
}

TypeCatalogBuildLimits makeTypeCatalogBuildLimits(const EffectiveResourceLimits & limits)
{
    TypeCatalogBuildLimits result;
    result.maximum_definitions = limits.get(ResourceLimit::DefinitionsPerDatabase);
    /// `maximum_root_accounted_bytes` measures allocator-dependent local memory,
    /// so it must stay independent of deterministic catalog admission charge.
    return result;
}

TemplateSpecializerLimits makeTemplateSpecializerLimits(const EffectiveResourceLimits & limits)
{
    TemplateSpecializerLimits result;
    result.maximum_canonical_argument_bytes = limits.get(ResourceLimit::CanonicalActualArgumentBytes);
    result.maximum_canonical_argument_item_bytes
        = std::min(result.maximum_canonical_argument_item_bytes, result.maximum_canonical_argument_bytes);
    result.maximum_work = limits.get(ResourceLimit::CheckerExpansionWorkUnits);
    return result;
}

TypeDescriptorLimits makeTypeDescriptorLimits(const EffectiveResourceLimits & limits)
{
    TypeDescriptorLimits result;
    result.maximum_canonical_arguments_bytes = limits.get(ResourceLimit::CanonicalActualArgumentBytes);
    result.maximum_path_depth = lower(result.maximum_path_depth, limits, ResourceLimit::LogicalPathDepth);
    result.maximum_descriptors = lower(result.maximum_descriptors, limits, ResourceLimit::DemandedDescriptorsPerQuery);
    return result;
}

TypeResolverLimits makeTypeResolverLimits(const EffectiveResourceLimits & limits)
{
    TypeResolverLimits result;
    result.maximum_physical_ast_nodes = limits.get(ResourceLimit::LoweredPhysicalTypeNodes);
    result.specializer = makeTemplateSpecializerLimits(limits);
    result.descriptors = makeTypeDescriptorLimits(limits);
    return result;
}

PersistedTypeReferencesLimits makePersistedTypeReferencesLimits(const EffectiveResourceLimits & limits)
{
    PersistedTypeReferencesLimits result;
    lowerPersistedTypeReferencesLimits(result, limits);
    return result;
}

TableColumnTypeBindingLimits makeTableColumnTypeBindingLimits(const EffectiveResourceLimits & limits)
{
    TableColumnTypeBindingLimits result;
    lowerPersistedTypeReferencesLimits(result.persisted, limits);
    result.maximum_descriptor_occurrences = lower(result.maximum_descriptor_occurrences, limits, ResourceLimit::OccurrencePathsPerObject);
    return result;
}

BoundObjectTypeReferencesLimits makeBoundObjectTypeReferencesLimits(const EffectiveResourceLimits & limits)
{
    BoundObjectTypeReferencesLimits result;
    lowerPersistedTypeReferencesLimits(result.persisted, limits);
    return result;
}

}
