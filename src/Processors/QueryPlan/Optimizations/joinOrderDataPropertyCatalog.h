#pragma once

#include <Processors/QueryPlan/Optimizations/DataProperties.h>

#include <Core/Block_fwd.h>
#include <base/types.h>

#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{

/// Narrowing guard shared by the join-order data property tables: their ids and offsets
/// are stored as `UInt32`, so every size that becomes one must be checked, with the
/// overflowing table named in the error.
UInt32 checkedJoinOrderUInt32(size_t value, std::string_view description);

/// Catalog-local strong identifier: one distinct type per interned definition table so ids
/// cannot be mixed up, without repeating the `{UInt32 value; operator<=>}` struct per table.
template <typename Tag>
struct JoinOrderCatalogId
{
    UInt32 value = 0;
    auto operator<=>(const JoinOrderCatalogId &) const = default;
};

struct JoinOrderColumnIdTag;
struct JoinOrderUniqueKeyIdTag;
struct JoinOrderFunctionalDependencyIdTag;
struct JoinOrderLineageIdTag;
struct JoinOrderNameIdTag;

using JoinOrderColumnId = JoinOrderCatalogId<JoinOrderColumnIdTag>;
using JoinOrderUniqueKeyId = JoinOrderCatalogId<JoinOrderUniqueKeyIdTag>;
using JoinOrderFunctionalDependencyId = JoinOrderCatalogId<JoinOrderFunctionalDependencyIdTag>;
using JoinOrderLineageId = JoinOrderCatalogId<JoinOrderLineageIdTag>;
using JoinOrderNameId = JoinOrderCatalogId<JoinOrderNameIdTag>;

struct JoinOrderFactRange
{
    UInt32 offset = 0;
    UInt32 size = 0;

    bool operator==(const JoinOrderFactRange &) const = default;
};

struct JoinOrderColumnDefinition
{
    UInt32 relation = 0;
    UInt32 leaf_output_position = 0;
    JoinOrderNameId display_name;
    JoinOrderNameId type_name;
    bool intrinsic_non_null = false;
};

struct JoinOrderUniqueKeyDefinition
{
    UInt32 relation = 0;
    JoinOrderFactRange columns;
    QueryPlanOptimizations::DataPropertyProvenance provenance;
    QueryPlanOptimizations::DataPropertyEqualityMode equality_mode = QueryPlanOptimizations::DataPropertyEqualityMode::Unsupported;
};

struct JoinOrderFunctionalDependencyDefinition
{
    UInt32 relation = 0;
    JoinOrderFactRange determinant;
    JoinOrderFactRange dependents;
    QueryPlanOptimizations::DataPropertyDependencyKind kind = QueryPlanOptimizations::DataPropertyDependencyKind::Exact;
    QueryPlanOptimizations::DataPropertyProvenance provenance;
};

struct JoinOrderLineageDefinition
{
    UInt32 relation = 0;
    JoinOrderColumnId output;
    UInt32 input_child_index = 0;
    UInt32 input_position = 0;
    JoinOrderNameId input_name;
    QueryPlanOptimizations::ColumnLineageKind kind = QueryPlanOptimizations::ColumnLineageKind::Unknown;
    QueryPlanOptimizations::DataPropertyProvenance provenance;
};

class JoinOrderDataPropertyCatalog
{
public:
    size_t relationCount() const { return relation_count; }
    size_t columnCount() const { return column_definitions.size(); }
    size_t uniqueKeyCount() const { return unique_key_definitions.size(); }
    size_t functionalDependencyCount() const { return functional_dependency_definitions.size(); }
    size_t lineageCount() const { return lineage_definitions.size(); }

    const JoinOrderColumnDefinition & column(JoinOrderColumnId id) const;
    const JoinOrderUniqueKeyDefinition & uniqueKey(JoinOrderUniqueKeyId id) const;
    const JoinOrderFunctionalDependencyDefinition & functionalDependency(JoinOrderFunctionalDependencyId id) const;
    const JoinOrderLineageDefinition & lineage(JoinOrderLineageId id) const;
    std::span<const JoinOrderColumnId> columns(JoinOrderFactRange range) const;
    const String & name(JoinOrderNameId id) const;
    const String & typeName(JoinOrderColumnId id) const;

    std::span<const JoinOrderColumnId> columnsForRelation(UInt32 relation) const;
    std::span<const JoinOrderUniqueKeyId> uniqueKeysForRelation(UInt32 relation) const;
    std::span<const JoinOrderFunctionalDependencyId> functionalDependenciesForRelation(UInt32 relation) const;
    std::span<const JoinOrderLineageId> lineageForRelation(UInt32 relation) const;

    bool isIntrinsicNonNull(JoinOrderColumnId id) const;
    bool isTrustedUniqueKey(JoinOrderUniqueKeyId id) const;

private:
    friend class JoinOrderDataPropertyCatalogBuilder;

    size_t relation_count = 0;
    std::vector<String> names;
    std::vector<JoinOrderColumnDefinition> column_definitions;
    std::vector<JoinOrderUniqueKeyDefinition> unique_key_definitions;
    std::vector<JoinOrderFunctionalDependencyDefinition> functional_dependency_definitions;
    std::vector<JoinOrderLineageDefinition> lineage_definitions;
    std::vector<JoinOrderColumnId> fact_columns;

    std::vector<JoinOrderColumnId> relation_columns;
    std::vector<JoinOrderUniqueKeyId> relation_unique_keys;
    std::vector<JoinOrderFunctionalDependencyId> relation_functional_dependencies;
    std::vector<JoinOrderLineageId> relation_lineage;
    std::vector<JoinOrderFactRange> relation_column_ranges;
    std::vector<JoinOrderFactRange> relation_unique_key_ranges;
    std::vector<JoinOrderFactRange> relation_functional_dependency_ranges;
    std::vector<JoinOrderFactRange> relation_lineage_ranges;
};

enum class JoinOrderDataPropertyCatalogMode : UInt8
{
    Costing,
    Diagnostics,
};

/// Canonical mapping from the collection policy to the catalog mode, so callers cannot
/// drift on the conversion. Callers must first gate on `policy.collectsDataProperties()`.
constexpr JoinOrderDataPropertyCatalogMode
joinOrderDataPropertyCatalogMode(const QueryPlanOptimizations::DataPropertyCollectionPolicy & policy)
{
    return policy.diagnostics_enabled ? JoinOrderDataPropertyCatalogMode::Diagnostics : JoinOrderDataPropertyCatalogMode::Costing;
}

class JoinOrderDataPropertyCatalogBuilder
{
public:
    JoinOrderDataPropertyCatalogBuilder();
    JoinOrderDataPropertyCatalogBuilder(JoinOrderDataPropertyCatalogBuilder &&) noexcept;
    JoinOrderDataPropertyCatalogBuilder & operator=(JoinOrderDataPropertyCatalogBuilder &&) noexcept;
    ~JoinOrderDataPropertyCatalogBuilder();

    JoinOrderDataPropertyCatalogBuilder(const JoinOrderDataPropertyCatalogBuilder &) = delete;
    JoinOrderDataPropertyCatalogBuilder & operator=(const JoinOrderDataPropertyCatalogBuilder &) = delete;

    JoinOrderDataPropertyCatalogBuilder & appendLeaf(const QueryPlanOptimizations::DataPropertySet & properties, const Block & leaf_header);
    JoinOrderDataPropertyCatalogBuilder & merge(JoinOrderDataPropertyCatalogBuilder && rhs);
    std::shared_ptr<const JoinOrderDataPropertyCatalog> finalize(JoinOrderDataPropertyCatalogMode mode) &&;
    size_t relationCount() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
