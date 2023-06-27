#pragma once

#include <vector>

#include <Core/Names.h>
#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTIdentifier;
class ASTQualifiedAsterisk;
struct ASTTableJoin;
class ASTSelectQuery;
class ASTExpressionList;
class ASTFunction;

/// Visit one node for names qualification. @sa InDepthNodeVisitor.
class TranslateQualifiedNamesMatcher
{
public:
    using Visitor = InDepthNodeVisitor<TranslateQualifiedNamesMatcher, true>;

    struct Data
    {
        const NameSet source_columns;
        const TablesWithColumns & tables;
        std::unordered_set<String> join_using_columns;
        bool has_columns;
        NameToNameMap parameter_values;
        NameToNameMap parameter_types;

        Data(const NameSet & source_columns_, const TablesWithColumns & tables_, bool has_columns_ = true, const NameToNameMap & parameter_values_ = {}, const NameToNameMap & parameter_types_ = {})
            : source_columns(source_columns_)
            , tables(tables_)
            , has_columns(has_columns_)
            , parameter_values(parameter_values_)
            , parameter_types(parameter_types_)
        {}

        bool hasColumn(const String & name) const { return source_columns.count(name); }
        bool hasTable() const { return !tables.empty(); }
        bool processAsterisks() const { return hasTable() && has_columns; }
        bool unknownColumn(size_t table_pos, const ASTIdentifier & identifier) const;
        static bool matchColumnName(std::string_view name, const String & column_name, DataTypePtr column_type);
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);

private:
    static void visit(ASTIdentifier & identifier, ASTPtr & ast, Data &);
    static void visit(const ASTQualifiedAsterisk & node, const ASTPtr & ast, Data &);
    static void visit(ASTTableJoin & join, const ASTPtr & ast, Data &);
    static void visit(ASTSelectQuery & select, const ASTPtr & ast, Data &);
    static void visit(ASTExpressionList &, const ASTPtr &, Data &);
    static void visit(ASTFunction &, const ASTPtr &, Data &);

    static void extractJoinUsingColumns(ASTPtr ast, Data & data);

};

/// Visits AST for names qualification.
/// It finds columns and translate their names to the normal form. Expand asterisks and qualified asterisks with column names.
using TranslateQualifiedNamesVisitor = TranslateQualifiedNamesMatcher::Visitor;


/// Restore ASTIdentifiers to long form, change table name in case of distributed.
struct RestoreQualifiedNamesMatcher
{
    struct Data
    {
        DatabaseAndTableWithAlias distributed_table;
        DatabaseAndTableWithAlias remote_table;

        void changeTable(ASTIdentifier & identifier) const;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & ast, Data & data);
    static void visit(ASTIdentifier & identifier, ASTPtr & ast, Data & data);
};

using RestoreQualifiedNamesVisitor = InDepthNodeVisitor<RestoreQualifiedNamesMatcher, true>;

/// Restore ASTIdentifier to long form, similar to RestoreQualifiedNamesMatcher, but without distributed table and works for one particular identifier.
/// It handles the case `SELECT a + 1 as a, a FROM t1 JOIN t2` where we need to replace first `a` with `t1.a`.
class RestoreQualifiedAliasedIdentifierMatcher
{
public:
    using Visitor = InDepthNodeVisitor<RestoreQualifiedAliasedIdentifierMatcher, true>;
    struct Data
    {
        const NameSet & source_columns;
        const TablesWithColumns & tables;
        String current_node_alias;

        bool hasColumn(const String & name) const { return source_columns.count(name); }
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child);
    static void visit(ASTPtr & ast, Data & data);
    static void visit(ASTIdentifier & identifier, ASTPtr & ast, Data & data);
};

using RestoreQualifiedAliasedIdentifierVisitor = RestoreQualifiedAliasedIdentifierMatcher::Visitor;

}
