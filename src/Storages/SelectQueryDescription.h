#pragma once

#include <Interpreters/StorageID.h>

namespace DB
{

/// Select query for different view in storages
struct SelectQueryDescription
{
    /// Table id for select query
    StorageID select_table_id = StorageID::createEmpty();
    /// Select query itself (ASTSelectWithUnionQuery)
    ASTPtr select_query;
    /// First query from select_query list
    ASTPtr inner_query;

    /// Parse description from select query for materialized view. Also
    /// validates query.
    static SelectQueryDescription getSelectQueryFromASTForMatView(const ASTPtr & select, const Context & context);

    SelectQueryDescription() = default;
    SelectQueryDescription(const SelectQueryDescription & other);
    SelectQueryDescription & operator=(const SelectQueryDescription & other);
};

}
