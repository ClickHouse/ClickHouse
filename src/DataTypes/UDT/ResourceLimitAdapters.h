#pragma once

#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/PersistedTypeReferences.h>
#include <DataTypes/UDT/ResourceLimits.h>
#include <DataTypes/UDT/TableColumnTypeBindings.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TemplateSpecializer.h>
#include <DataTypes/UDT/TypeResolver.h>

namespace DB::UDT
{

/// Hard aggregate TemplateChecker domains implied by the ResourceLimits
/// implementation maxima (100k definitions, 4096 nodes, 256 direct edges,
/// 1024 transitive dependencies, 65536 per-definition proof work, and 1 GiB
/// deterministic catalog bytes). Closure enumeration is separately charged
/// without changing the frozen checker-charge ABI stored in durable records.
inline constexpr UInt64 template_checker_implementation_maximum_catalog_nodes = 409'600'000;
inline constexpr UInt64 template_checker_implementation_maximum_catalog_edges = 25'600'000;
inline constexpr UInt64 template_checker_implementation_maximum_catalog_work = 32'870'700'000;
inline constexpr UInt64 template_checker_implementation_maximum_scratch_bytes = 4ULL << 30;

/// Narrow mappings for existing admission boundaries. A field is changed only
/// when the common contract has the same charge identity; component-specific
/// finite limits remain at their reviewed defaults.
ResourceLimitLayer makeAuthorityResourceLimitLayer(const TypeAuthorityLimits & limits);
EffectiveResourceLimits makeDefaultQueryEffectiveResourceLimits(const TypeAuthorityLimits & authority_limits);
/// Joins the exact immutable database-generation tuple with the query/profile
/// and current adapter layers. This can only retain or tighten the persisted
/// implementation/server/database minimum.
EffectiveResourceLimits
makeQueryEffectiveResourceLimits(const EffectiveResourceLimits & effective_database_limits, const TypeAuthorityLimits & authority_limits);
TemplateCheckerLimits makeTemplateCheckerLimits(const EffectiveResourceLimits & limits);
TypeCatalogBuildLimits makeTypeCatalogBuildLimits(const EffectiveResourceLimits & limits);
TemplateSpecializerLimits makeTemplateSpecializerLimits(const EffectiveResourceLimits & limits);
TypeDescriptorLimits makeTypeDescriptorLimits(const EffectiveResourceLimits & limits);
TypeResolverLimits makeTypeResolverLimits(const EffectiveResourceLimits & limits);
void lowerPersistedTypeReferencesLimits(PersistedTypeReferencesLimits & result, const EffectiveResourceLimits & limits) noexcept;
PersistedTypeReferencesLimits makePersistedTypeReferencesLimits(const EffectiveResourceLimits & limits);
TableColumnTypeBindingLimits makeTableColumnTypeBindingLimits(const EffectiveResourceLimits & limits);
BoundObjectTypeReferencesLimits makeBoundObjectTypeReferencesLimits(const EffectiveResourceLimits & limits);

}
