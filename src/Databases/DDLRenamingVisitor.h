#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Core/QualifiedTableName.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <memory>
#include <optional>
#include <unordered_map>


namespace DB
{
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class DDLRenamingMap;
class ASTFunction;
class ASTIdentifier;
class ASTSelectQuery;
class ColumnsDescription;

/// Changes names of databases or tables in a create query according to a specified renaming map.
/// Does not validate AST, works a best-effort way.
void renameDatabaseAndTableNameInCreateQuery(ASTPtr ast, const DDLRenamingMap & renaming_map, const ContextPtr & global_context);

/// Renaming map keeps information about new names of databases or tables.
class DDLRenamingMap
{
public:
    void setNewTableName(const QualifiedTableName & old_table_name, const QualifiedTableName & new_table_name);
    void setNewDatabaseName(const String & old_database_name, const String & new_database_name);

    QualifiedTableName getNewTableName(const QualifiedTableName & old_table_name) const;
    String getNewDatabaseName(const String & old_database_name) const;

private:
    std::unordered_map<QualifiedTableName, QualifiedTableName> old_to_new_table_names;
    std::unordered_map<String, String> old_to_new_database_names;
};

/// Visits ASTCreateQuery and changes names of databases or tables.
class DDLRenamingVisitor
{
public:
    struct Data
    {
        ASTPtr create_query;
        const DDLRenamingMap & renaming_map;
        ContextPtr global_context;
    };

    using Visitor = InDepthNodeVisitor<DDLRenamingVisitor, false>;

    static bool needChildVisit(const ASTPtr &, const ASTPtr &);
    static void visit(ASTPtr ast, const Data & data);
};

/// Renames a database in the definition of a view or a TimeSeries table: in its table references, as
/// `renameDatabaseAndTableNameInCreateQuery` does, and in the names `db.table.column`, `db.table.*` and
/// `db.table.COLUMNS(...)` bound to a table expression `db.table` of their scope.
class DatabaseRenamingInViewDefinition
{
public:
    /// Analyzes `create_query`; `view_name` is the table it defines, whose database qualifies table names written without one.
    DatabaseRenamingInViewDefinition(ASTPtr create_query_, String old_database_name_, String new_database_name_, QualifiedTableName view_name_);

    /// The tables whose columns `check` reads.
    std::vector<QualifiedTableName> getTablesToCheck() const;

    /// Throws AMBIGUOUS_IDENTIFIER unless the rename provably keeps what every name of the definition refers to
    /// and the names of the columns of the view (`view_columns`).
    void check(const ColumnsDescription & view_columns, const ContextPtr & context) const;

    /// Renames in place.
    void apply(const ContextPtr & global_context);

    /// Throws NOT_IMPLEMENTED if, after `apply`, a table function still names the old database: in an argument
    /// the rename does not rewrite, or as a parameterized view call it cannot represent.
    void checkTableFunctions() const;

private:
    /// A name written in the definition outside the renamed positions, split into components on '.'.
    struct WrittenName
    {
        Strings components;
        bool multi_component = false;
    };

    /// A table expression of a SELECT from the one of a name to rename to the one holding the name's table.
    struct ScopeTableExpression
    {
        String renamed_name;
        /// Empty if it is not a table name (a subquery, a table function).
        Strings table_name_parts;
    };

    /// Where a name is written: an expression, a table reference, or the qualifier of `table.*`.
    enum class NamePosition : uint8_t
    {
        Expression,
        Table,
        Qualifier,
    };

    void collect(const ASTPtr & ast, NamePosition position);
    void collectIdentifier(ASTIdentifier & identifier, NamePosition position);
    void addName(const Strings & parts, bool multi_component = false);
    void addTableArgument(const ASTFunction & function, const IAST & argument);
    std::optional<size_t> findTableExpressionScope(const String & table_name) const;
    std::vector<QualifiedTableName> getScopeTables(bool throw_if_not_table) const;
    [[noreturn]] void throwAmbiguous(const String & reason) const;

    ASTPtr create_query;
    String old_database_name;
    String new_database_name;
    QualifiedTableName view_name;

    std::vector<WrittenName> names;
    /// The renamed tables on the right side of `IN`, and the renamed first arguments of `dictGet` and `joinGet` as literals,
    /// as the analyzer writes them in column names.
    NameSet in_tables;
    NameSet function_literals;
    std::vector<ASTIdentifier *> candidates;
    std::vector<ScopeTableExpression> scope_table_expressions;
    NameSet cte_names;
    std::vector<const ASTSelectQuery *> current_scopes;
};

}
