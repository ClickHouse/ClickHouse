#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

namespace DB
{

/// Select query for different view in storages
struct SelectQueryDescription
{
    /// Table id for select query. Only for non-refreshable materialized views.
    StorageID select_table_id = StorageID::createEmpty();
    /// Select query itself (ASTSelectWithUnionQuery)
    ASTPtr select_query;
    /// First query from select_query list
    ASTPtr inner_query;

    /// Parse description from select query for materialized view. Also
    /// validates query.
    static SelectQueryDescription getSelectQueryFromASTForMatView(const ASTPtr & select, bool refreshable, ContextPtr context);

    /// Whether any SETTINGS clause of the query sets or resets `enable_global_with_statement`.
    static bool fixesGlobalWithSetting(const IAST & select);
    /// A materialized view's query is analyzed and executed with `enable_global_with_statement` always enabled
    /// (the registered source table must not depend on the executing session). Rejects fixing it in a fresh
    /// definition; a replayed Replicated database entry is not rejected (its initiator already accepted it).
    static void checkSettingsAllowedInMatView(const IAST & select, const ContextPtr & context);

    SelectQueryDescription() = default;
    SelectQueryDescription(const SelectQueryDescription & other);
    SelectQueryDescription & operator=(const SelectQueryDescription & other);
    SelectQueryDescription(SelectQueryDescription && other) noexcept = default;
    SelectQueryDescription & operator=(SelectQueryDescription && other) noexcept = default;
};

}
