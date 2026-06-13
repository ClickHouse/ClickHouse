#pragma once

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{

class ASTSelectQuery;
class TableJoin;
struct SelectQueryOptions;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

/// Joined tables' columns resolver.
/// We want to get each table structure at most once per table occurrence. Or even better once per table.
/// TODO: joins tree with costs to change joins order by CBO.
class JoinedTables
{
public:
    JoinedTables(ContextPtr context, const ASTSelectQuery & select_query_, bool include_all_columns_ = false, bool is_create_parameterized_view_ = false);

    void reset(const ASTSelectQuery & select_query);

    StoragePtr getLeftTableStorage();
    bool resolveTables();

    /// Make fake tables_with_columns[0] in case we have predefined input in InterpreterSelectQuery
    void makeFakeTable(StoragePtr storage, const StorageMetadataPtr & metadata_snapshot, const Block & source_header);
    std::shared_ptr<TableJoin> makeTableJoin(const ASTSelectQuery & select_query);

    const TablesWithColumns & tablesWithColumns() const { return tables_with_columns; }

    /// The left table id exactly as written in the query (after database/UUID resolution but
    /// before resolving an `Overlay` facade to the underlying source table). Empty until
    /// `getLeftTableStorage` has been called for a plain table reference. Used to check access
    /// against the facade name in addition to the underlying source database.
    const StorageID & leftTableStorageID() const { return left_table_id; }

    bool isLeftTableSubquery() const;
    bool isLeftTableFunction() const;
    size_t tablesCount() const { return table_expressions.size(); }

    void rewriteDistributedInAndJoins(ASTPtr & query);

    std::unique_ptr<InterpreterSelectWithUnionQuery> makeLeftTableSubquery(const SelectQueryOptions & select_options);

private:
    ContextPtr context;
    std::vector<const ASTTableExpression *> table_expressions;
    TablesWithColumns tables_with_columns;
    const bool include_all_columns;

    /// Legacy (duplicated left table values)
    ASTPtr left_table_expression;
    std::optional<DatabaseAndTableWithAlias> left_db_and_table;
    StorageID left_table_id = StorageID::createEmpty();
    const ASTSelectQuery & select_query;
    const bool is_create_parameterized_view;
};

}
