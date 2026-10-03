#include <Storages/MergeTree/ReplacingTTLCoverage.h>

#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TTLDescription.h>

#include <algorithm>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool replacing_ttl_whole_partition_only;
}

namespace
{

/// Stored columns that have the same value in all versions of a key within a partition: the columns
/// of the sorting key, which identifies the key, and the columns that are the partition key or its
/// elements. A key expression such as `toDate(ts)` is not a stored column, and `ts` may differ
/// between the versions.
NameSet getColumnsSharedByVersions(const StorageInMemoryMetadata & metadata)
{
    NameSet result;
    const auto & columns = metadata.getColumns();

    const auto add_stored = [&](const Names & names)
    {
        for (const auto & name : names)
        {
            if (columns.hasPhysical(name))
                result.insert(name);
        }
    };

    add_stored(metadata.getSortingKeyColumns());
    add_stored(metadata.getPartitionKey().column_names);
    return result;
}

bool readsOnly(const NamesAndTypesList & columns, const NameSet & allowed)
{
    return std::ranges::all_of(columns, [&](const NameAndTypePair & column) { return allowed.contains(column.name); });
}

/// `INTERVAL n unit` with a positive `n`.
bool isPositiveInterval(const ASTPtr & ast)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || !function->name.starts_with("toInterval") || !function->arguments || function->arguments->children.size() != 1)
        return false;

    const auto * literal = function->arguments->children[0]->as<ASTLiteral>();
    return literal && literal->value.getType() == Field::Types::UInt64 && literal->value.safeGet<UInt64>() > 0;
}

/// `version + INTERVAL n unit [+ INTERVAL m unit ...]`, the arguments of `plus` in any order.
/// A positive interval keeps the value away from 0, which a `TTL` treats as "no TTL", and adding it keeps the order of
/// version, except that an interval of a day or longer is added in local time, so around the hour that repeats when
/// clocks go back, a `DateTime` version can get a `TTL` up to that hour later than a newer version.
bool isVersionPlusPositiveIntervals(const ASTPtr & ast, const String & version_column)
{
    const auto * function = ast->as<ASTFunction>();
    if (!function || function->name != "plus" || !function->arguments || function->arguments->children.size() != 2)
        return false;

    const auto is_version_term = [&](const ASTPtr & node)
    {
        if (const auto * identifier = node->as<ASTIdentifier>())
            return identifier->name() == version_column;

        return isVersionPlusPositiveIntervals(node, version_column);
    };

    const auto & lhs = function->arguments->children[0];
    const auto & rhs = function->arguments->children[1];
    return (is_version_term(lhs) && isPositiveInterval(rhs)) || (isPositiveInterval(lhs) && is_version_term(rhs));
}

/// True if an older version of a key has expired by `ttl` whenever a newer version has.
bool isSafeForAnySubsetOfVersions(const TTLDescription & ttl, const NameSet & shared_columns, const String & version_column, bool version_is_date)
{
    if (ttl.where_expression_ast && !readsOnly(ttl.where_expression_columns, shared_columns))
        return false;

    if (readsOnly(ttl.expression_columns, shared_columns))
        return true;

    return version_is_date && isVersionPlusPositiveIntervals(ttl.expression_ast, version_column);
}

}

bool rowTTLNeedsWholePartitionMerge(const StorageInMemoryMetadata & metadata, const MergeTreeData::MergingParams & merging_params, const MergeTreeSettings & settings)
{
    if (merging_params.mode != MergeTreeData::MergingParams::Replacing)
        return false;

    if (!settings[MergeTreeSetting::replacing_ttl_whole_partition_only])
        return false;

    const auto & table_ttl = metadata.table_ttl;
    if (!table_ttl.rows_ttl.expression_ast && table_ttl.rows_where_ttl.empty())
        return false;

    const NameSet shared_columns = getColumnsSharedByVersions(metadata);

    bool version_is_date{false};
    if (!merging_params.version_column.empty())
    {
        auto version = metadata.getColumns().tryGetPhysical(merging_params.version_column);
        version_is_date = version && isDateOrDate32OrDateTimeOrDateTime64(version->type);
    }

    if (table_ttl.rows_ttl.expression_ast && !isSafeForAnySubsetOfVersions(table_ttl.rows_ttl, shared_columns, merging_params.version_column, version_is_date))
        return true;

    for (const auto & ttl : table_ttl.rows_where_ttl)
    {
        if (!isSafeForAnySubsetOfVersions(ttl, shared_columns, merging_params.version_column, version_is_date))
            return true;
    }

    return false;
}
}
