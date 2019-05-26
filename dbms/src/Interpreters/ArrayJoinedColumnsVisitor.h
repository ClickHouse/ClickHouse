#pragma once

#include <Core/Names.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <DataTypes/NestedUtils.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/Aliases.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ALIAS_REQUIRED;
    extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
    extern const int LOGICAL_ERROR;
}

/// Fills the array_join_result_to_source: on which columns-arrays to replicate, and how to call them after that.
class ArrayJoinedColumnsMatcher
{
public:
    using Visitor = InDepthNodeVisitor<ArrayJoinedColumnsMatcher, true>;

    struct Data
    {
        const Aliases & aliases;
        NameToNameMap & array_join_name_to_alias;
        NameToNameMap & array_join_alias_to_name;
        NameToNameMap & array_join_result_to_source;
    };

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child)
    {
        if (node->as<ASTTablesInSelectQuery>())
            return false;

        if (child->as<ASTSubquery>() || child->as<ASTSelectQuery>())
            return false;

        return true;
    }

    static void visit(ASTPtr & ast, Data & data)
    {
        if (const auto * t = ast->as<ASTIdentifier>())
            visit(*t, ast, data);
        if (const auto * t = ast->as<ASTSelectQuery>())
            visit(*t, ast, data);
    }

private:
    static void visit(const ASTSelectQuery & node, ASTPtr &, Data & data)
    {
        ASTPtr array_join_expression_list = node.array_join_expression_list();
        if (!array_join_expression_list)
            throw Exception("Logical error: no ARRAY JOIN", ErrorCodes::LOGICAL_ERROR);

        std::vector<ASTPtr *> out;
        out.reserve(array_join_expression_list->children.size());

        for (ASTPtr & ast : array_join_expression_list->children)
        {
            const String nested_table_name = ast->getColumnName();
            const String nested_table_alias = ast->getAliasOrColumnName();

            if (nested_table_alias == nested_table_name && !ast->as<ASTIdentifier>())
                throw Exception("No alias for non-trivial value in ARRAY JOIN: " + nested_table_name, ErrorCodes::ALIAS_REQUIRED);

            if (data.array_join_alias_to_name.count(nested_table_alias) || data.aliases.count(nested_table_alias))
                throw Exception("Duplicate alias in ARRAY JOIN: " + nested_table_alias, ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

            data.array_join_alias_to_name[nested_table_alias] = nested_table_name;
            data.array_join_name_to_alias[nested_table_name] = nested_table_alias;

            for (ASTPtr & child2 : ast->children)
                out.emplace_back(&child2);
        }

        for (ASTPtr * add_node : out)
            Visitor(data).visit(*add_node);
    }

    static void visit(const ASTIdentifier & node, ASTPtr &, Data & data)
    {
        NameToNameMap & array_join_name_to_alias = data.array_join_name_to_alias;
        NameToNameMap & array_join_alias_to_name = data.array_join_alias_to_name;
        NameToNameMap & array_join_result_to_source = data.array_join_result_to_source;

        if (!IdentifierSemantic::getColumnName(node))
            return;

        auto splitted = Nested::splitName(node.name);  /// ParsedParams, Key1

        if (array_join_alias_to_name.count(node.name))
        {
            /// ARRAY JOIN was written with an array column. Example: SELECT K1 FROM ... ARRAY JOIN ParsedParams.Key1 AS K1
            array_join_result_to_source[node.name] = array_join_alias_to_name[node.name];    /// K1 -> ParsedParams.Key1
        }
        else if (array_join_alias_to_name.count(splitted.first) && !splitted.second.empty())
        {
            /// ARRAY JOIN was written with a nested table. Example: SELECT PP.KEY1 FROM ... ARRAY JOIN ParsedParams AS PP
            array_join_result_to_source[node.name]    /// PP.Key1 -> ParsedParams.Key1
                = Nested::concatenateName(array_join_alias_to_name[splitted.first], splitted.second);
        }
        else if (array_join_name_to_alias.count(node.name))
        {
            /** Example: SELECT ParsedParams.Key1 FROM ... ARRAY JOIN ParsedParams.Key1 AS PP.Key1.
            * That is, the query uses the original array, replicated by itself.
            */
            array_join_result_to_source[    /// PP.Key1 -> ParsedParams.Key1
                array_join_name_to_alias[node.name]] = node.name;
        }
        else if (array_join_name_to_alias.count(splitted.first) && !splitted.second.empty())
        {
            /** Example: SELECT ParsedParams.Key1 FROM ... ARRAY JOIN ParsedParams AS PP.
            */
            array_join_result_to_source[    /// PP.Key1 -> ParsedParams.Key1
                Nested::concatenateName(array_join_name_to_alias[splitted.first], splitted.second)] = node.name;
        }
    }
};

using ArrayJoinedColumnsVisitor = ArrayJoinedColumnsMatcher::Visitor;

}
