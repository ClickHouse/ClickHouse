#include <Storages/getEffectiveRowPolicyFilter.h>

#include <Access/Common/RowPolicyDefs.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionContainsColumnMatcher.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool analyzer_compatibility_prefer_alias_over_subcolumn;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void checkRowPolicyFilterMatchersAgainstStorage(const ASTPtr & filter, const IStorage & storage, const ContextPtr & context)
{
    if (!filter)
        return;

    auto storage_id = storage.getStorageID();
    StorageMetadataHandle metadata_snapshot;
    auto is_column_qualifier = makeTupleColumnQualifierCheck(
        [&](const String & name)
        {
            /// The columns are needed only for a qualified matcher, which is rare in a row policy.
            if (!metadata_snapshot)
                metadata_snapshot = storage.getInMemoryMetadataPtr(context, /* bypass_metadata_cache = */ false);
            return metadata_snapshot->getColumns().tryGetColumn(GetColumnsOptions(GetColumnsOptions::All).withSubcolumns(), name);
        },
        NameSet{storage_id.getTableName()},
        std::make_pair(storage_id.getDatabaseName(), storage_id.getTableName()),
        context->getSettingsRef()[Setting::analyzer_compatibility_prefer_alias_over_subcolumn]);

    if (auto matcher = findColumnMatcherInExpression(filter, is_column_qualifier))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Column matcher {} is not allowed in a row policy filter expression; list the columns explicitly. In filter {}",
            matcher->formatForErrorMessage(),
            filter->formatForErrorMessage());
}

namespace
{

/// The visited set terminates the walk: a storage contributes its policies once, even if the chain of underlying storages loops.
void collectRowPolicyFilters(const IStorage & storage, const ContextPtr & context, RowPolicyFilterPtr & result, NameSet & visited)
{
    auto storage_id = storage.getStorageID();
    if (!storage_id.hasDatabase() || !visited.emplace(storage_id.getFullTableName()).second)
        return;

    auto filter = context->getRowPolicyFilter(storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
    /// `ContextAccess::getRowPolicyFilter` does not know the columns of the table, so it lets a qualified matcher
    /// through. Check it here, against the columns, before the filter reaches any of its consumers.
    if (filter)
        checkRowPolicyFilterMatchersAgainstStorage(filter->expression, storage, context);

    result = combineRowPolicyFilters(std::move(result), std::move(filter));

    for (const auto & underlying : storage.getUnderlyingStorages())
        if (underlying)
            collectRowPolicyFilters(*underlying, context, result, visited);
}

}

RowPolicyFilterPtr getRowPolicyFilterForStorage(const IStorage & storage, const ContextPtr & context)
{
    RowPolicyFilterPtr result;
    NameSet visited;
    collectRowPolicyFilters(storage, context, result, visited);
    return result;
}

RowPolicyFilterPtr getEffectiveRowPolicyFilter(const IStorage & storage, const ContextPtr & context)
{
    auto filter = getRowPolicyFilterForStorage(storage, context);
    if (!filter || filter->isAlwaysTrue())
        return nullptr;
    return filter;
}

}
