#include <Processors/QueryPlan/Optimizations/joinOrderDataPropertyCatalog.h>

#include <Core/Block.h>
#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

UInt32 checkedJoinOrderUInt32(size_t value, std::string_view description)
{
    if (value > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Too many {} for join-order data properties: {}", description, value);
    return static_cast<UInt32>(value);
}

namespace
{

using QueryPlanOptimizations::ColumnLineageKind;
using QueryPlanOptimizations::DataPropertyDependencyKind;
using QueryPlanOptimizations::DataPropertyEqualityMode;
using QueryPlanOptimizations::DataPropertyProvenance;
using QueryPlanOptimizations::SortingScope;

template <typename T>
const T & checkedGet(const std::vector<T> & values, UInt32 index, std::string_view description)
{
    if (index >= values.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid join-order {} id {} (size {})", description, index, values.size());
    return values[index];
}

template <typename T>
std::span<const T> checkedSpan(const std::vector<T> & values, JoinOrderFactRange range, std::string_view description)
{
    const size_t offset = range.offset;
    const size_t size = range.size;
    if (offset > values.size() || size > values.size() - offset)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Invalid join-order {} range [{}, {}) for size {}",
            description,
            offset,
            offset + size,
            values.size());
    return std::span<const T>(values).subspan(offset, size);
}

}

const JoinOrderColumnDefinition & JoinOrderDataPropertyCatalog::column(JoinOrderColumnId id) const
{
    return checkedGet(column_definitions, id.value, "column");
}

const JoinOrderUniqueKeyDefinition & JoinOrderDataPropertyCatalog::uniqueKey(JoinOrderUniqueKeyId id) const
{
    return checkedGet(unique_key_definitions, id.value, "unique key");
}

const JoinOrderFunctionalDependencyDefinition & JoinOrderDataPropertyCatalog::functionalDependency(JoinOrderFunctionalDependencyId id) const
{
    return checkedGet(functional_dependency_definitions, id.value, "functional dependency");
}

const JoinOrderLineageDefinition & JoinOrderDataPropertyCatalog::lineage(JoinOrderLineageId id) const
{
    return checkedGet(lineage_definitions, id.value, "lineage");
}

const JoinOrderSortingDefinition & JoinOrderDataPropertyCatalog::sorting(JoinOrderSortingId id) const
{
    return checkedGet(sorting_definitions, id.value, "sorting");
}

std::span<const JoinOrderColumnId> JoinOrderDataPropertyCatalog::columns(JoinOrderFactRange range) const
{
    return checkedSpan(fact_columns, range, "fact columns");
}

std::span<const JoinOrderSortColumnDefinition> JoinOrderDataPropertyCatalog::sortColumns(JoinOrderFactRange range) const
{
    return checkedSpan(sort_column_definitions, range, "sort columns");
}

const String & JoinOrderDataPropertyCatalog::name(JoinOrderNameId id) const
{
    return checkedGet(names, id.value, "name");
}

const String & JoinOrderDataPropertyCatalog::typeName(JoinOrderColumnId id) const
{
    return name(column(id).type_name);
}

std::span<const JoinOrderColumnId> JoinOrderDataPropertyCatalog::columnsForRelation(UInt32 relation) const
{
    return checkedSpan(relation_columns, checkedGet(relation_column_ranges, relation, "relation column range"), "relation columns");
}

std::span<const JoinOrderUniqueKeyId> JoinOrderDataPropertyCatalog::uniqueKeysForRelation(UInt32 relation) const
{
    return checkedSpan(
        relation_unique_keys, checkedGet(relation_unique_key_ranges, relation, "relation unique-key range"), "relation unique keys");
}

std::span<const JoinOrderFunctionalDependencyId> JoinOrderDataPropertyCatalog::functionalDependenciesForRelation(UInt32 relation) const
{
    return checkedSpan(
        relation_functional_dependencies,
        checkedGet(relation_functional_dependency_ranges, relation, "relation functional-dependency range"),
        "relation functional dependencies");
}

std::span<const JoinOrderLineageId> JoinOrderDataPropertyCatalog::lineageForRelation(UInt32 relation) const
{
    return checkedSpan(relation_lineage, checkedGet(relation_lineage_ranges, relation, "relation lineage range"), "relation lineage");
}

std::optional<JoinOrderSortingId> JoinOrderDataPropertyCatalog::sortingForRelation(UInt32 relation) const
{
    return checkedGet(relation_sorting, relation, "relation sorting");
}

bool JoinOrderDataPropertyCatalog::isIntrinsicNonNull(JoinOrderColumnId id) const
{
    return column(id).intrinsic_non_null;
}

bool JoinOrderDataPropertyCatalog::isTrustedUniqueKey(JoinOrderUniqueKeyId id) const
{
    const auto & key = uniqueKey(id);
    return QueryPlanOptimizations::isProvenStrongBagKey(key.provenance, key.equality_mode);
}

struct JoinOrderDataPropertyCatalogBuilder::Impl
{
    struct LocalKey
    {
        std::vector<UInt32> columns;
        DataPropertyProvenance provenance;
        DataPropertyEqualityMode equality_mode = DataPropertyEqualityMode::Unsupported;
    };

    struct LocalFunctionalDependency
    {
        std::vector<UInt32> determinant;
        std::vector<UInt32> dependents;
        DataPropertyDependencyKind kind = DataPropertyDependencyKind::Exact;
        DataPropertyProvenance provenance;
    };

    struct LocalLineage
    {
        UInt32 output = 0;
        UInt32 input_child_index = 0;
        UInt32 input_position = 0;
        String input_name;
        ColumnLineageKind kind = ColumnLineageKind::Unknown;
        DataPropertyProvenance provenance;
    };

    struct LocalSortColumn
    {
        UInt32 column = 0;
        Int8 direction = 1;
        Int8 nulls_direction = 1;
        std::optional<String> collation_locale;
    };

    struct LocalSorting
    {
        std::vector<LocalSortColumn> columns;
        SortingScope scope{SortingScope::Stream};
    };

    struct Segment
    {
        std::vector<String> column_names;
        std::vector<String> column_types;
        std::vector<bool> non_null;
        std::vector<LocalKey> unique_keys;
        std::vector<LocalFunctionalDependency> functional_dependencies;
        std::vector<LocalLineage> lineage;
        std::optional<LocalSorting> sorting;
    };

    std::vector<Segment> segments;
    bool finalized = false;
};

JoinOrderDataPropertyCatalogBuilder::JoinOrderDataPropertyCatalogBuilder()
    : impl(std::make_unique<Impl>())
{
}

JoinOrderDataPropertyCatalogBuilder::JoinOrderDataPropertyCatalogBuilder(JoinOrderDataPropertyCatalogBuilder &&) noexcept = default;
JoinOrderDataPropertyCatalogBuilder & JoinOrderDataPropertyCatalogBuilder::operator=(JoinOrderDataPropertyCatalogBuilder &&) noexcept
    = default;
JoinOrderDataPropertyCatalogBuilder::~JoinOrderDataPropertyCatalogBuilder() = default;

JoinOrderDataPropertyCatalogBuilder &
JoinOrderDataPropertyCatalogBuilder::appendLeaf(const QueryPlanOptimizations::DataPropertySet & properties, const Block & leaf_header)
{
    if (!impl || impl->finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot append to a finalized join-order data property catalog builder");

    Impl::Segment segment;
    checkedJoinOrderUInt32(leaf_header.columns(), "leaf columns");
    segment.column_names.reserve(leaf_header.columns());
    segment.column_types.reserve(leaf_header.columns());
    segment.non_null.assign(leaf_header.columns(), false);
    for (const auto & column : leaf_header)
    {
        segment.column_names.push_back(column.name);
        segment.column_types.push_back(column.type->getName());
    }

    auto map_columns = [&](const QueryPlanOptimizations::ColumnSet & columns) -> std::optional<std::vector<UInt32>>
    {
        std::vector<UInt32> mapped;
        mapped.reserve(columns.size());
        for (const auto & column : columns)
        {
            if (column.position >= segment.column_names.size() || segment.column_names[column.position] != column.name)
                return {};
            mapped.push_back(checkedJoinOrderUInt32(column.position, "leaf output positions"));
        }
        std::ranges::sort(mapped);
        mapped.erase(std::unique(mapped.begin(), mapped.end()), mapped.end());
        return mapped;
    };

    for (const auto & column : properties.nonNullColumns())
    {
        if (column.position < segment.column_names.size() && segment.column_names[column.position] == column.name)
            segment.non_null[column.position] = true;
    }

    for (const auto & key : properties.uniqueKeys())
    {
        auto mapped = map_columns(key.columns);
        if (mapped && !mapped->empty())
            segment.unique_keys.push_back({std::move(*mapped), key.provenance, key.equality_mode});
    }

    for (const auto & dependency : properties.functionalDependencies())
    {
        auto determinant = map_columns(dependency.determinant);
        auto dependents = map_columns(dependency.dependents);
        if (determinant && dependents && !determinant->empty() && !dependents->empty())
            segment.functional_dependencies.push_back(
                {std::move(*determinant), std::move(*dependents), dependency.kind, dependency.provenance});
    }

    for (const auto & fact : properties.columnLineage())
    {
        if (fact.output.position >= segment.column_names.size() || segment.column_names[fact.output.position] != fact.output.name)
            continue;
        segment.lineage.push_back(
            {checkedJoinOrderUInt32(fact.output.position, "lineage output positions"),
             checkedJoinOrderUInt32(fact.input.child_index, "lineage child indexes"),
             checkedJoinOrderUInt32(fact.input.position, "lineage input positions"),
             fact.input.name,
             fact.kind,
             fact.provenance});
    }

    if (!properties.sorting().empty())
    {
        Impl::LocalSorting sorting;
        sorting.scope = properties.sorting().sort_scope;
        sorting.columns.reserve(properties.sorting().sort_description.size());
        bool valid = true;
        for (const auto & sort_column : properties.sorting().sort_description)
        {
            std::optional<UInt32> position;
            for (size_t candidate = 0; candidate < segment.column_names.size(); ++candidate)
            {
                if (segment.column_names[candidate] != sort_column.column_name)
                    continue;
                if (position)
                {
                    valid = false;
                    break;
                }
                position = checkedJoinOrderUInt32(candidate, "sorting output positions");
            }
            if (!valid || !position || (sort_column.direction != 1 && sort_column.direction != -1)
                || (sort_column.nulls_direction != 1 && sort_column.nulls_direction != -1))
            {
                valid = false;
                break;
            }
            sorting.columns.push_back(
                {*position,
                 static_cast<Int8>(sort_column.direction),
                 static_cast<Int8>(sort_column.nulls_direction),
                 sort_column.collator ? std::optional<String>(sort_column.collator->getLocale()) : std::nullopt});
        }
        if (valid && !sorting.columns.empty())
            segment.sorting = std::move(sorting);
    }

    impl->segments.push_back(std::move(segment));
    return *this;
}

JoinOrderDataPropertyCatalogBuilder & JoinOrderDataPropertyCatalogBuilder::merge(JoinOrderDataPropertyCatalogBuilder && rhs)
{
    if (!impl || impl->finalized || !rhs.impl || rhs.impl->finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot append finalized join-order data property catalog builders");
    impl->segments.insert(
        impl->segments.end(), std::make_move_iterator(rhs.impl->segments.begin()), std::make_move_iterator(rhs.impl->segments.end()));
    rhs.impl->segments.clear();
    return *this;
}

size_t JoinOrderDataPropertyCatalogBuilder::relationCount() const
{
    if (!impl)
        return 0;
    return impl->segments.size();
}

std::shared_ptr<const JoinOrderDataPropertyCatalog> JoinOrderDataPropertyCatalogBuilder::finalize(JoinOrderDataPropertyCatalogMode mode) &&
{
    const bool include_diagnostic_facts = mode == JoinOrderDataPropertyCatalogMode::Diagnostics;
    if (!impl || impl->finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Join-order data property catalog builder was finalized more than once");
    impl->finalized = true;
    checkedJoinOrderUInt32(impl->segments.size(), "relations");

    auto catalog = std::make_shared<JoinOrderDataPropertyCatalog>();
    catalog->relation_count = impl->segments.size();
    catalog->relation_column_ranges.reserve(catalog->relation_count);
    catalog->relation_unique_key_ranges.reserve(catalog->relation_count);
    catalog->relation_functional_dependency_ranges.reserve(catalog->relation_count);
    catalog->relation_lineage_ranges.reserve(catalog->relation_count);
    catalog->relation_sorting.reserve(catalog->relation_count);

    std::unordered_map<String, JoinOrderNameId> name_ids;
    auto intern_name = [&](const String & value)
    {
        if (auto it = name_ids.find(value); it != name_ids.end())
            return it->second;
        JoinOrderNameId id{checkedJoinOrderUInt32(catalog->names.size(), "names")};
        catalog->names.push_back(value);
        name_ids.emplace(catalog->names.back(), id);
        return id;
    };

    auto append_fact_columns = [&](const std::vector<UInt32> & local_columns, const std::vector<JoinOrderColumnId> & column_ids)
    {
        JoinOrderFactRange range{
            checkedJoinOrderUInt32(catalog->fact_columns.size(), "fact columns"),
            checkedJoinOrderUInt32(local_columns.size(), "fact columns")};
        for (UInt32 local : local_columns)
        {
            if (local >= column_ids.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid local join-order column {} (size {})", local, column_ids.size());
            catalog->fact_columns.push_back(column_ids[local]);
        }
        return range;
    };

    for (UInt32 relation = 0; relation < impl->segments.size(); ++relation)
    {
        const auto & segment = impl->segments[relation];
        if (segment.column_names.size() != segment.column_types.size() || segment.column_names.size() != segment.non_null.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Inconsistent join-order catalog column metadata for relation {}", relation);
        std::vector<JoinOrderColumnId> column_ids;
        column_ids.reserve(segment.column_names.size());

        JoinOrderFactRange column_range{
            checkedJoinOrderUInt32(catalog->relation_columns.size(), "relation columns"),
            checkedJoinOrderUInt32(segment.column_names.size(), "relation columns")};
        for (UInt32 position = 0; position < segment.column_names.size(); ++position)
        {
            JoinOrderColumnId id{checkedJoinOrderUInt32(catalog->column_definitions.size(), "columns")};
            catalog->column_definitions.push_back(
                {relation,
                 position,
                 intern_name(segment.column_names[position]),
                 intern_name(segment.column_types[position]),
                 segment.non_null[position]});
            catalog->relation_columns.push_back(id);
            column_ids.push_back(id);
        }
        catalog->relation_column_ranges.push_back(column_range);

        std::optional<JoinOrderSortingId> sorting_id;
        if (segment.sorting)
        {
            JoinOrderFactRange sorting_columns{
                checkedJoinOrderUInt32(catalog->sort_column_definitions.size(), "sort columns"),
                checkedJoinOrderUInt32(segment.sorting->columns.size(), "sort columns")};
            for (const auto & local_sort_column : segment.sorting->columns)
            {
                if (local_sort_column.column >= column_ids.size())
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Invalid local join-order sorting column {} (size {})",
                        local_sort_column.column,
                        column_ids.size());
                std::optional<JoinOrderNameId> collation_locale;
                if (local_sort_column.collation_locale)
                    collation_locale = intern_name(*local_sort_column.collation_locale);
                catalog->sort_column_definitions.push_back(
                    {column_ids[local_sort_column.column],
                     local_sort_column.direction,
                     local_sort_column.nulls_direction,
                     collation_locale});
            }
            sorting_id = JoinOrderSortingId{checkedJoinOrderUInt32(catalog->sorting_definitions.size(), "sorting properties")};
            catalog->sorting_definitions.push_back({relation, sorting_columns, segment.sorting->scope});
        }
        catalog->relation_sorting.push_back(sorting_id);

        const UInt32 key_offset = checkedJoinOrderUInt32(catalog->relation_unique_keys.size(), "relation unique keys");
        for (const auto & local_key : segment.unique_keys)
        {
            if (!include_diagnostic_facts && !QueryPlanOptimizations::isProvenStrongBagKey(local_key.provenance, local_key.equality_mode))
                continue;
            JoinOrderUniqueKeyId id{checkedJoinOrderUInt32(catalog->unique_key_definitions.size(), "unique keys")};
            catalog->unique_key_definitions.push_back(
                {relation, append_fact_columns(local_key.columns, column_ids), local_key.provenance, local_key.equality_mode});
            catalog->relation_unique_keys.push_back(id);
        }
        catalog->relation_unique_key_ranges.push_back(
            {key_offset, checkedJoinOrderUInt32(catalog->relation_unique_keys.size() - key_offset, "relation unique keys")});

        const UInt32 dependency_offset
            = checkedJoinOrderUInt32(catalog->relation_functional_dependencies.size(), "relation functional dependencies");
        if (include_diagnostic_facts)
        {
            for (const auto & local_dependency : segment.functional_dependencies)
            {
                JoinOrderFunctionalDependencyId id{
                    checkedJoinOrderUInt32(catalog->functional_dependency_definitions.size(), "functional dependencies")};
                catalog->functional_dependency_definitions.push_back(
                    {relation,
                     append_fact_columns(local_dependency.determinant, column_ids),
                     append_fact_columns(local_dependency.dependents, column_ids),
                     local_dependency.kind,
                     local_dependency.provenance});
                catalog->relation_functional_dependencies.push_back(id);
            }
        }
        catalog->relation_functional_dependency_ranges.push_back(
            {dependency_offset,
             checkedJoinOrderUInt32(
                 catalog->relation_functional_dependencies.size() - dependency_offset, "relation functional dependencies")});

        const UInt32 lineage_offset = checkedJoinOrderUInt32(catalog->relation_lineage.size(), "relation lineage");
        for (const auto & local_lineage : segment.lineage)
        {
            const bool needed_for_binding
                = local_lineage.kind == ColumnLineageKind::Identity || local_lineage.kind == ColumnLineageKind::ValuePreserving;
            if (!include_diagnostic_facts && !needed_for_binding)
                continue;
            if (local_lineage.output >= column_ids.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid local lineage output {}", local_lineage.output);
            JoinOrderLineageId id{checkedJoinOrderUInt32(catalog->lineage_definitions.size(), "lineage facts")};
            catalog->lineage_definitions.push_back(
                {relation,
                 column_ids[local_lineage.output],
                 local_lineage.input_child_index,
                 local_lineage.input_position,
                 intern_name(local_lineage.input_name),
                 local_lineage.kind,
                 local_lineage.provenance});
            catalog->relation_lineage.push_back(id);
        }
        catalog->relation_lineage_ranges.push_back(
            {lineage_offset, checkedJoinOrderUInt32(catalog->relation_lineage.size() - lineage_offset, "relation lineage")});
    }

    return catalog;
}

}
