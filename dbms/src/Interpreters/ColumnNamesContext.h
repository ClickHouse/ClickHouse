#pragma once

#include <ostream>
#include <optional>

#include <Common/typeid_cast.h>
#include <Core/Names.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace DB
{

/// Information about table and column names extracted from ASTSelectQuery block. Do not include info from subselects.
struct ColumnNamesContext
{
    struct JoinedTable
    {
        const ASTTableExpression * expr = nullptr;
        const ASTTableJoin * join = nullptr;

        std::optional<String> alias() const
        {
            String alias;
            if (expr)
            {
                if (expr->database_and_table_name)
                    alias = expr->database_and_table_name->tryGetAlias();
                else if (expr->table_function)
                    alias = expr->table_function->tryGetAlias();
                else if (expr->subquery)
                    alias = expr->subquery->tryGetAlias();
            }
            if (!alias.empty())
                return alias;
            return {};
        }

        std::optional<String> name() const
        {
            if (expr)
                return getIdentifierName(expr->database_and_table_name);
            return {};
        }

        std::optional<ASTTableJoin::Kind> joinKind() const
        {
            if (join)
                return join->kind;
            return {};
        }
    };

    std::unordered_map<String, std::set<String>> required_names; /// names with aliases
    NameSet table_aliases;
    NameSet private_aliases;
    NameSet complex_aliases;
    NameSet masked_columns;
    NameSet array_join_columns;
    std::vector<JoinedTable> tables; /// ordered list of visited tables in FROM section with joins
    bool has_table_join = false;
    bool has_array_join = false;

    bool addTableAliasIfAny(const IAST & ast);
    bool addColumnAliasIfAny(const IAST & ast);
    void addColumnIdentifier(const ASTIdentifier & node);
    bool addArrayJoinAliasIfAny(const IAST & ast);
    void addArrayJoinIdentifier(const ASTIdentifier & node);

    NameSet requiredColumns() const;
};

std::ostream & operator << (std::ostream & os, const ColumnNamesContext & cols);

}
