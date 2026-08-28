#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/defines.h>
#include <cstring>

namespace DB
{

namespace
{

/// Create a Range from statistics estimate for use in part pruning.
/// MinMax statistics now store typed Field values, so we can directly construct Range
/// without lossy Float64 conversions.
///
/// NULL handling: a Nullable column's NULL values sort at POSITIVE_INFINITY. When the NULL
/// count is known (`Basic` statistics on a nullable column), the range can be tightened:
///   - null_count == 0: no NULLs in the part, the right bound is the real max;
///   - null_count == rows_count: the part is all-NULL, represented by the [+inf, +inf] sentinel
///     range (it intersects nothing except ranges that reach the NULL sentinel);
///   - otherwise the right bound stays POSITIVE_INFINITY to cover possible NULLs.
///
/// Returns std::nullopt when statistics are unavailable or corrupted,
/// causing the caller to fall back to a whole-universe Range (no pruning).
std::optional<Range> createRangeFromEstimate(const Estimate & estimate, const DataTypePtr & /*data_type*/, bool is_nullable)
{
    if (estimate.rows_count == 0)
        return std::nullopt;

    const std::optional<UInt64> & null_count = estimate.estimated_null_count;
    if (null_count.has_value() && *null_count > estimate.rows_count)
        return std::nullopt; /// corrupted statistics

    if (estimate.estimated_min.has_value() && estimate.estimated_max.has_value())
    {
        const Field & min_value = estimate.estimated_min.value();
        const Field & max_value = estimate.estimated_max.value();

        /// min > max is a legacy sentinel pair or corrupted statistics; only an all-NULL
        /// part of a nullable column yields a prunable range here.
        if (min_value > max_value)
        {
            if (is_nullable && null_count.has_value() && *null_count == estimate.rows_count)
                return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
            return std::nullopt;
        }

        if (is_nullable && null_count.has_value() && *null_count == estimate.rows_count)
            return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);

        if (!is_nullable || (null_count.has_value() && *null_count == 0))
            return Range(min_value, true, max_value, true);

        /// Nullable column that may contain NULLs: keep the right bound at the NULL sentinel.
        return Range(min_value, true, POSITIVE_INFINITY, true);
    }

    /// No min/max (non-numeric type like String/Array/Tuple/Map, or an all-NULL part):
    /// the NULL count alone can still produce a useful range for a nullable column.
    if (is_nullable && null_count.has_value())
    {
        if (*null_count == estimate.rows_count)
            return Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
        if (*null_count == 0)
            return Range::createWholeUniverseWithoutNull();
        /// Partial NULLs cannot be expressed as a single continuous Range.
        return std::nullopt;
    }

    return std::nullopt;
}

/// Returns true when a column's statistics description can produce a useful range for part
/// pruning: numeric min/max values (an explicit `MinMax` statistic, or `Basic` on a
/// numeric/temporal column), or a NULL count (`Basic` on a nullable column). Used before part
/// statistics are loaded to decide whether part pruning can be beneficial at all.
bool statisticsSupportsPartPruning(const ColumnStatisticsDescription & stats_desc)
{
    if (stats_desc.types_to_desc.contains(StatisticsType::MinMax))
        return true;
    if (stats_desc.types_to_desc.contains(StatisticsType::Basic))
        return removeLowCardinalityAndNullable(stats_desc.data_type)->isValueRepresentedByNumber()
            || isNullableOrLowCardinalityNullable(stats_desc.data_type);
    return false;
}

/// Collect names of `.null` subcolumns whose parent column has `Basic` statistics and a
/// nullable type, so that bare boolean inputs on them can be rewritten to comparisons.
NameSet collectNullSubcolumnsToNormalize(const StorageMetadataPtr & metadata)
{
    NameSet result;
    if (!metadata)
        return result;
    for (const auto & col : metadata->getColumns())
    {
        if (col.statistics.types_to_desc.contains(StatisticsType::Basic)
            && isNullableOrLowCardinalityNullable(col.type))
            result.insert(col.name + ".null");
    }
    return result;
}

/// If `name` is a `<parent>.null` subcolumn of a nullable column with `Basic` statistics,
/// return the parent column name. A physical column literally named `foo.null` is never
/// treated as a virtual key: the physical-column check wins.
std::optional<String> tryResolveVirtualKeyParent(const StorageMetadataPtr & metadata, const String & name)
{
    if (!name.ends_with(".null"))
        return std::nullopt;
    const auto & columns = metadata->getColumns();
    if (columns.tryGet(name))
        return std::nullopt; /// physical column
    String parent = name.substr(0, name.size() - strlen(".null"));
    if (const auto * parent_col = columns.tryGet(parent))
    {
        if (parent_col->statistics.types_to_desc.contains(StatisticsType::Basic)
            && isNullableOrLowCardinalityNullable(parent_col->type))
            return parent;
    }
    return std::nullopt;
}

/// Create a Range on the virtual UInt8 `.null` subcolumn from the parent column's NULL
/// count: value 0 marks "row is not NULL", value 1 marks "row is NULL".
std::optional<Range> createRangeFromNullCount(const Estimate & estimate)
{
    if (!estimate.estimated_null_count.has_value() || estimate.rows_count == 0
        || *estimate.estimated_null_count > estimate.rows_count)
        return std::nullopt;

    UInt64 null_count = *estimate.estimated_null_count;
    if (null_count == 0)
        return Range(UInt64(0), true, UInt64(0), true);
    if (null_count == estimate.rows_count)
        return Range(UInt64(1), true, UInt64(1), true);
    return Range(UInt64(0), true, UInt64(1), true);
}

} /// anonymous namespace

StatisticsPartPruner::StatisticsPartPruner(const StorageMetadataPtr & metadata_, const ActionsDAG::Node & filter_node_, ContextPtr context_)
    : null_subcolumns_to_normalize(collectNullSubcolumnsToNormalize(metadata_))
    , filter_dag(&filter_node_, context_, /* boolean_context */ true,
                 null_subcolumns_to_normalize.empty() ? nullptr : &null_subcolumns_to_normalize)
    , context(context_)
{
    if (!metadata_ || !filter_dag.dag)
        return;

    const auto & columns = metadata_->getColumns();
    Names filter_columns = filter_dag.dag->getRequiredColumnsNames();

    for (const auto & name : filter_columns)
    {
        if (const auto * col = columns.tryGet(name))
        {
            if (statisticsSupportsPartPruning(col->statistics))
            {
                stats_column_name_to_type_map[col->name] = col->type;
                useless = false;
            }
            continue;
        }

        /// Virtual `.null` key produced by `optimize_functions_to_subcolumns`: register it
        /// as a UInt8 key column resolved against the parent column's estimate.
        if (auto parent = tryResolveVirtualKeyParent(metadata_, name))
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

    /// Pruning estimates must not run a query pipeline: only state that is already computed may be
    /// read here.
    auto new_key_condition = std::make_unique<KeyCondition>(
        filter_dag, context, column_names, expression,
        /* single_point_ */ false, /* skip_analysis_ */ false, /* require_ready_sets_ */ true);

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
    /// Filter to estimates that can produce a useful range: numeric min/max values or a
    /// NULL count. An all-NULL part has no min/max at all, so gating on `estimated_min`
    /// alone would silently drop the only evidence we have for such parts.
    Estimates pruning_estimates;
    for (const auto & [col_name, estimate] : estimates)
    {
        if (estimate.estimated_min.has_value() || estimate.estimated_null_count.has_value())
            pruning_estimates[col_name] = estimate;
    }

    if (pruning_estimates.empty())
        return {true, true};

    /// Use only columns that are both in filter and have estimates. Virtual `.null` keys
    /// resolve to their parent column's estimate.
    NamesAndTypesList columns;
    for (const auto & [col_name, col_type] : stats_column_name_to_type_map)
    {
        auto parent_it = virtual_key_to_parent.find(col_name);
        const String & estimate_name = parent_it != virtual_key_to_parent.end() ? parent_it->second : col_name;
        if (pruning_estimates.contains(estimate_name))
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
        auto parent_it = virtual_key_to_parent.find(col_name);
        bool is_virtual_null_key = parent_it != virtual_key_to_parent.end();
        const String & estimate_name = is_virtual_null_key ? parent_it->second : col_name;

        auto est_it = pruning_estimates.find(estimate_name);
        chassert(est_it != pruning_estimates.end());

        std::optional<Range> range;
        if (is_virtual_null_key)
            range = createRangeFromNullCount(est_it->second);
        else
            range = createRangeFromEstimate(est_it->second, col_type, isNullableOrLowCardinalityNullable(col_type));

        if (range.has_value())
        {
            hyperrectangle.push_back(std::move(*range));
        }
        else if (is_virtual_null_key)
        {
            /// A `.null` subcolumn is plain UInt8 and never NULL itself.
            hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        }
        else if (isNullableOrLowCardinalityNullable(col_type))
        {
            hyperrectangle.emplace_back(Range::createWholeUniverse());
        }
        else
        {
            hyperrectangle.emplace_back(Range::createWholeUniverseWithoutNull());
        }
        types.push_back(col_type);
    }

    return key_condition->checkInHyperrectangle(hyperrectangle, types);
}

}
