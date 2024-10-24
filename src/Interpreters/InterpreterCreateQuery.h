#pragma once

#include <Core/NamesAndAliases.h>
#include <Access/Common/AccessRightsElement.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/IInterpreter.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class ASTCreateQuery;
class ASTColumnDeclaration;
class ASTExpressionList;
class ASTConstraintDeclaration;
class ASTStorage;
class IDatabase;
class DDLGuard;
using DatabasePtr = std::shared_ptr<IDatabase>;
using DDLGuardPtr = std::unique_ptr<DDLGuard>;


/** Allows to create new table or database,
  *  or create an object for existing table or database.
  */
class InterpreterCreateQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_);

    BlockIO execute() override;

    /// List of columns and their types in AST.
    static ASTPtr formatColumns(const NamesAndTypesList & columns);
    static ASTPtr formatColumns(const NamesAndTypesList & columns, const NamesAndAliases & alias_columns);
    static ASTPtr formatColumns(const ColumnsDescription & columns);
    static ASTPtr formatIndices(const IndicesDescription & indices);
    static ASTPtr formatConstraints(const ConstraintsDescription & constraints);
    static ASTPtr formatProjections(const ProjectionsDescription & projections);

    void setForceRestoreData(bool has_force_restore_data_flag_)
    {
        has_force_restore_data_flag = has_force_restore_data_flag_;
    }

    void setInternal(bool internal_)
    {
        internal = internal_;
    }

    void setForceAttach(bool force_attach_)
    {
        force_attach = force_attach_;
    }

    void setLoadDatabaseWithoutTables(bool load_database_without_tables_)
    {
        load_database_without_tables = load_database_without_tables_;
    }

    void setDontNeedDDLGuard()
    {
        need_ddl_guard = false;
    }

    void setIsRestoreFromBackup(bool is_restore_from_backup_)
    {
        is_restore_from_backup = is_restore_from_backup_;
    }

    static DataTypePtr getColumnType(const ASTColumnDeclaration & col_decl, LoadingStrictnessLevel mode, bool make_columns_nullable);

    /// Obtain information about columns, their types, default values and column comments,
    ///  for case when columns in CREATE query is specified explicitly.
    static ColumnsDescription getColumnsDescription(const ASTExpressionList & columns, ContextPtr context, LoadingStrictnessLevel mode, bool is_restore_from_backup = false);
    static ConstraintsDescription
    getConstraintsDescription(const ASTExpressionList * constraints, const ColumnsDescription & columns, ContextPtr local_context);

    static void prepareOnClusterQuery(ASTCreateQuery & create, ContextPtr context, const String & cluster_name);

    void extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & ast, ContextPtr) const override;

    /// Check access right, validate definer statement and replace `CURRENT USER` with actual name.
    static void processSQLSecurityOption(
        ContextPtr context_, ASTSQLSecurity & sql_security, bool is_materialized_view = false, bool skip_check_permissions = false);

private:
    struct TableProperties
    {
        ColumnsDescription columns;
        IndicesDescription indices;
        ConstraintsDescription constraints;
        ProjectionsDescription projections;
        bool columns_inferred_from_select_query = false;
    };

    BlockIO createDatabase(ASTCreateQuery & create);
    BlockIO createTable(ASTCreateQuery & create);

    /// Calculate list of columns, constraints, indices, etc... of table. Rewrite query in canonical way.
    TableProperties getTablePropertiesAndNormalizeCreateQuery(ASTCreateQuery & create, LoadingStrictnessLevel mode) const;
    void validateTableStructure(const ASTCreateQuery & create, const TableProperties & properties) const;
    void validateMaterializedViewColumnsAndEngine(const ASTCreateQuery & create, const TableProperties & properties, const DatabasePtr & database);
    void setEngine(ASTCreateQuery & create) const;
    AccessRightsElements getRequiredAccess() const;

    /// Create IStorage and add it to database. If table already exists and IF NOT EXISTS specified, do nothing and return false.
    bool doCreateTable(ASTCreateQuery & create, const TableProperties & properties, DDLGuardPtr & ddl_guard, LoadingStrictnessLevel mode);
    BlockIO doCreateOrReplaceTable(ASTCreateQuery & create, const InterpreterCreateQuery::TableProperties & properties, LoadingStrictnessLevel mode);
    /// Inserts data in created table if it's CREATE ... SELECT
    BlockIO fillTableIfNeeded(const ASTCreateQuery & create);

    void assertOrSetUUID(ASTCreateQuery & create, const DatabasePtr & database) const;

    /// Update create query with columns description from storage if query doesn't have it.
    /// It's used to prevent automatic schema inference while table creation on each server startup.
    void addColumnsDescriptionToCreateQueryIfNecessary(ASTCreateQuery & create, const StoragePtr & storage);

    BlockIO executeQueryOnCluster(ASTCreateQuery & create);

    ASTPtr query_ptr;

    /// Skip safety threshold when loading tables.
    bool has_force_restore_data_flag = false;
    /// Is this an internal query - not from the user.
    bool internal = false;
    bool force_attach = false;
    bool load_database_without_tables = false;
    bool need_ddl_guard = true;
    bool is_restore_from_backup = false;

    mutable String as_database_saved;
    mutable String as_table_saved;
};
}
