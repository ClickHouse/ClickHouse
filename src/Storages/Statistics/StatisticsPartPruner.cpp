#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>

namespace DB
{

namespace
{

/// Create a `Range` for `KeyCondition` from a column's MinMax and/or NullCount statistics.
///
/// Behaviour by case:
///   - Empty part (`rows_count == 0`): returns `std::nullopt`; caller falls back to a whole-universe range.
///   - No MinMax but NullCount present on a Nullable column:
///       * all NULL:          returns `[+inf, +inf]` (the NULL sentinel) so `IS NOT NULL` prunes.
///       * no NULLs:          returns the whole-universe range without NULLs so `IS NULL` prunes.
///       * partially NULL:    falls through to `std::nullopt` (no useful range without MinMax).
///   - MinMax present:
///       * `max < min` (sentinel or corruption): if NullCount says the part is all NULL, return
///         the NULL sentinel range; otherwise return `std::nullopt`.
///       * Non-Nullable column, or Nullable + NullCount confirms no NULLs: returns `[min, max]`.
///       * Otherwise (Nullable, may contain NULLs): returns `[min, +inf]` to cover the NULL sentinel.
std::optional<Range> createRangeFromEstimate(const Estimate & estimate, const DataTypePtr & /*data_type*/, bool is_nullable)
{
    if (estimate.rows_count == 0)
        return std::nullopt;

    const bool has_minmax = estimate.estimated_min.has_value() && estimate.estimated_max.has_value();
    const bool has_null_count = estimate.estimated_null_count.has_value();

    /// NullCount-only path for Nullable columns: no MinMax bounds available, but null-count
    /// alone can be enough when the part is entirely NULL or entirely non-NULL.
    if (!has_minmax && has_null_count && is_nullable)
    {
        UInt64 null_count = *estimate.estimated_null_count;
        if (null_count > estimate.rows_count)
            return std::nullopt; /// corrupted statistics — fall back to whole-universe.
        if (null_count == estimate.rows_count)
        {
            /// `KeyCondition` models NULL as a `POSITIVE_INFINITY` sentinel for Nullable key ranges.
            return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
        }
        if (null_count == 0)
            return Range::createWholeUniverseWithoutNull();
    }

    if (!has_minmax)
        return std::nullopt;

    const Field & min_value = estimate.estimated_min.value();
    const Field & max_value = estimate.estimated_max.value();

    /// `max < min` can happen with sentinel values written for all-NULL parts or with corrupted
    /// statistics. If NullCount also says the part is entirely NULL, we can still emit the
    /// `[+inf, +inf]` sentinel range to let `IS NOT NULL` prune. Otherwise give up.
    if (Range::less(max_value, min_value))
    {
        if (is_nullable && has_null_count && *estimate.estimated_null_count == estimate.rows_count)
            return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
        return std::nullopt;
    }

    /// Tight range `[min, max]` is safe when the column is non-Nullable or when NullCount
    /// confirms the part has no NULL values. Otherwise extend the right bound to `+inf` to
    /// cover the NULL sentinel.
    if (!is_nullable || (has_null_count && *estimate.estimated_null_count == 0))
        return Range(min_value, true, max_value, true);

    return Range(min_value, true, POSITIVE_INFINITY, true);
}

/// Create a `Range` for the virtual `.null` UInt8 subcolumn from the parent column's NullCount.
/// Returned ranges: `[0, 0]` (no NULLs), `[1, 1]` (all NULLs), `[0, 1]` (mixed).
std::optional<Range> createRangeFromNullCount(const Estimate & estimate)
{
    if (!estimate.estimated_null_count.has_value() || estimate.rows_count == 0)
        return std::nullopt;

    UInt64 null_count = *estimate.estimated_null_count;

    if (null_count > estimate.rows_count)
        return std::nullopt;
    if (null_count == 0)
        return Range(UInt64(0), true, UInt64(0), true);
    if (null_count == estimate.rows_count)
        return Range(UInt64(1), true, UInt64(1), true);
    return Range(UInt64(0), true, UInt64(1), true);
}

std::optional<String> tryResolveVirtualKeyParent(const String & subcolumn_name, const ColumnsDescription & columns)
{
    auto dot_pos = subcolumn_name.rfind('.');
    if (dot_pos == std::string::npos)
        return std::nullopt;

    /// Only `.null` subcolumns are supported as virtual keys today.
    if (subcolumn_name.compare(dot_pos + 1, std::string::npos, "null") != 0)
        return std::nullopt;

    String parent_name = subcolumn_name.substr(0, dot_pos);

    const auto * column = columns.tryGet(parent_name);
    if (!column)
        return std::nullopt;

    if (column->statistics.types_to_desc.contains(StatisticsType::NullCount)
        && isNullableOrLowCardinalityNullable(column->type))
        return parent_name;

    return std::nullopt;
}

/// Build the set of `.null` subcolumn names safe to normalize for part pruning: the parent
/// column must be Nullable (or LowCardinality(Nullable)) and carry a `NullCount` statistic.
/// This excludes user columns that merely happen to be named like `foo.null`.
NameSet collectNullSubcolumnsToNormalize(const StorageMetadataPtr & metadata)
{
    NameSet result;
    if (!metadata)
        return result;

    const auto & columns = metadata->getColumns();
    for (const auto & column : columns)
    {
        if (column.statistics.types_to_desc.contains(StatisticsType::NullCount)
            && isNullableOrLowCardinalityNullable(column.type))
        {
            result.insert(column.name + ".null");
        }
    }
    return result;
}

} /// anonymous namespace

StatisticsPartPruner::StatisticsPartPruner(const StorageMetadataPtr & metadata_, const ActionsDAG::Node & filter_node_, ContextPtr context_)
    : null_subcolumns_to_normalize(collectNullSubcolumnsToNormalize(metadata_))
    , filter_dag(&filter_node_, context_, &null_subcolumns_to_normalize)
    , context(context_)
{
    if (!metadata_ || !filter_dag.dag)
        return;

    const auto & columns = metadata_->getColumns();
    Names filter_columns = filter_dag.dag->getRequiredColumnsNames();

    for (const auto & name : filter_columns)
    {
        if (const auto * column = columns.tryGet(name))
        {
            if (column->statistics.types_to_desc.contains(StatisticsType::MinMax)
                || column->statistics.types_to_desc.contains(StatisticsType::NullCount))
            {
                stats_column_name_to_type_map[column->name] = column->type;
                useless = false;
            }
        }
        else if (auto parent = tryResolveVirtualKeyParent(name, columns))
        {
            stats_column_name_to_type_map[name] = std::make_shared<DataTypeUInt8>();
            virtual_key_to_parent[name] = *parent;
            useless = false;
        }
    }
}

KeyCondition * StatisticsPartPruner::getKeyConditionForEstimates(const NamesAndTypesList & columns)
{
    const auto column_names = columns.getNames();

    auto it = key_condition_cache.find(column_names);
    if (it != key_condition_cache.end())
        return it->second.get();

    ActionsDAG actions_dag(columns);
    auto expression = std::make_shared<ExpressionActions>(std::move(actions_dag));

    auto new_key_condition = std::make_unique<KeyCondition>(filter_dag, context, column_names, expression);

    if (new_key_condition->alwaysUnknownOrTrue())
    {
        key_condition_cache[column_names] = nullptr;
        return nullptr;
    }

    auto * key_condition_ptr = new_key_condition.get();
    key_condition_cache[column_names] = std::move(new_key_condition);

    for (size_t col_idx : key_condition_ptr->getUsedColumns())
    {
        if (col_idx < column_names.size())
            used_column_names.insert(column_names[col_idx]);
    }

    return key_condition_ptr;
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates)
{
    Estimates relevant_estimates;
    for (const auto & [col_name, estimate] : estimates)
    {
        if (estimate.types.contains(StatisticsType::MinMax)
            || estimate.types.contains(StatisticsType::NullCount))
            relevant_estimates[col_name] = estimate;
    }

    if (relevant_estimates.empty())
        return {true, true};

    NamesAndTypesList columns;
    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
    {
        auto virtual_it = virtual_key_to_parent.find(col_name);
        const String & estimate_name = virtual_it != virtual_key_to_parent.end() ? virtual_it->second : col_name;
        if (relevant_estimates.contains(estimate_name))
            columns.emplace_back(col_name, col_type);
    }

    if (columns.empty())
        return {true, true};

    KeyCondition * key_condition = getKeyConditionForEstimates(columns);
    if (!key_condition)
        return {true, true};

    Hyperrectangle hyperrectangle;
    DataTypes types;

    for (const auto & [col_name, col_type] : columns)
    {
        auto virtual_it = virtual_key_to_parent.find(col_name);
        if (virtual_it != virtual_key_to_parent.end())
        {
            auto est_it = relevant_estimates.find(virtual_it->second);
            auto range = est_it != relevant_estimates.end() ? createRangeFromNullCount(est_it->second) : std::nullopt;
            /// The `.null` virtual key is a bare UInt8 (never NULL), so the fallback excludes NULLs.
            hyperrectangle.emplace_back(range.has_value() ? std::move(*range) : Range::createWholeUniverseWithoutNull());
            types.push_back(col_type);
            continue;
        }

        auto est_it = relevant_estimates.find(col_name);
        chassert(est_it != relevant_estimates.end());

        auto is_nullable_type = isNullableOrLowCardinalityNullable(col_type);
        auto range = createRangeFromEstimate(est_it->second, col_type, is_nullable_type);

        if (range.has_value())
            hyperrectangle.push_back(std::move(*range));
        else
        {
            if (is_nullable_type)
                hyperrectangle.emplace_back(Range::createWholeUniverse());
            else
                hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        }
        types.push_back(col_type);
    }

    return key_condition->checkInHyperrectangle(hyperrectangle, types);
}

}
