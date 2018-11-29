#pragma once

#include <Core/Names.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <DataTypes/NestedUtils.h>

namespace DB
{

/// Visitors consist of functions with unified interface 'void visit(Casted & x, ASTPtr & y)', there x is y, successfully casted to Casted.
/// Both types and fuction could have const specifiers. The second argument is used by visitor to replaces AST node (y) if needed.

/// Fills the array_join_result_to_source: on which columns-arrays to replicate, and how to call them after that.
class ArrayJoinedColumnsVisitor
{
public:
    ArrayJoinedColumnsVisitor(NameToNameMap & array_join_name_to_alias_,
                              NameToNameMap & array_join_alias_to_name_,
                              NameToNameMap & array_join_result_to_source_)
    :   array_join_name_to_alias(array_join_name_to_alias_),
        array_join_alias_to_name(array_join_alias_to_name_),
        array_join_result_to_source(array_join_result_to_source_)
    {}

    void visit(ASTPtr & ast) const
    {
        if (!tryVisit<ASTTablesInSelectQuery>(ast) &&
            !tryVisit<ASTIdentifier>(ast))
            visitChildren(ast);
    }

private:
    NameToNameMap & array_join_name_to_alias;
    NameToNameMap & array_join_alias_to_name;
    NameToNameMap & array_join_result_to_source;

    void visit(const ASTTablesInSelectQuery &, ASTPtr &) const
    {}

    void visit(const ASTIdentifier & node, ASTPtr &) const
    {
        if (!node.general())
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

    void visit(const ASTSubquery &, ASTPtr &) const
    {}

    void visit(const ASTSelectQuery &, ASTPtr &) const
    {}

    void visitChildren(ASTPtr & ast) const
    {
        for (auto & child : ast->children)
            if (!tryVisit<ASTSubquery>(child) &&
                !tryVisit<ASTSelectQuery>(child))
                visit(child);
    }

    template <typename T>
    bool tryVisit(ASTPtr & ast) const
    {
        if (const T * t = typeid_cast<const T *>(ast.get()))
        {
            visit(*t, ast);
            return true;
        }
        return false;
    }
};

}
