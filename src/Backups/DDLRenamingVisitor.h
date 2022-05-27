#pragma once

#include <Core/Types.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTBackupQuery.h>
#include <map>
#include <memory>
#include <unordered_map>


namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Keeps information about renamings of databases or tables being processed
/// while we're making a backup or while we're restoring from a backup.
class DDLRenamingSettings
{
public:
    DDLRenamingSettings() = default;

    void setNewTableName(const DatabaseAndTableName & old_table_name, const DatabaseAndTableName & new_table_name);
    void setNewDatabaseName(const String & old_database_name, const String & new_database_name);

    void setFromBackupQuery(const ASTBackupQuery & backup_query);
    void setFromBackupQuery(const ASTBackupQuery::Elements & backup_query_elements);

    /// Changes names according to the renaming.
    DatabaseAndTableName getNewTableName(const DatabaseAndTableName & old_table_name) const;
    const String & getNewDatabaseName(const String & old_database_name) const;

private:
    std::map<DatabaseAndTableName, DatabaseAndTableName> old_to_new_table_names;
    std::unordered_map<String, String> old_to_new_database_names;
};


/// Changes names in AST according to the renaming settings.
void renameInCreateQuery(ASTPtr & ast, const ContextPtr & global_context, const DDLRenamingSettings & renaming_settings);

/// Visits ASTCreateQuery and changes names of tables and databases according to passed DDLRenamingConfig.
class DDLRenamingVisitor
{
public:
    struct Data
    {
        const DDLRenamingSettings & renaming_settings;
        ContextPtr context;
    };

    using Visitor = InDepthNodeVisitor<DDLRenamingVisitor, false>;

    static bool needChildVisit(ASTPtr &, const ASTPtr &);
    static void visit(ASTPtr & ast, const Data & data);
};

}
