#pragma once

#include <Core/Names.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTIdentifier.h>

#include <DataTypes/NestedUtils.h>
#include <Interpreters/InDepthNodeVisitor.h>


namespace DB
{

/// Fills the array_join_result_to_source: on which columns-arrays to replicate, and how to call them after that.
class ArrayJoinedColumnsMatcher
{
public:
    struct Data
    {
        NameToNameMap & array_join_name_to_alias;
        NameToNameMap & array_join_alias_to_name;
        NameToNameMap & array_join_result_to_source;
    };

    static constexpr const char * label = "ArrayJoinedColumns";

    static bool needChildVisit(ASTPtr & node, const ASTPtr & child)
    {
        /// Processed
        if (typeid_cast<ASTIdentifier *>(node.get()))
            return false;

        if (typeid_cast<ASTTablesInSelectQuery *>(node.get()))
            return false;

        if (typeid_cast<ASTSubquery *>(child.get()) ||
            typeid_cast<ASTSelectQuery *>(child.get()))
            return false;

        return true;
    }

    static std::vector<ASTPtr *> visit(ASTPtr & ast, Data & data)
    {
        if (auto * t = typeid_cast<ASTIdentifier *>(ast.get()))
            visit(*t, ast, data);
        return {};
    }

private:
    static void visit(const ASTIdentifier & node, ASTPtr &, Data & data)
    {
        NameToNameMap & array_join_name_to_alias = data.array_join_name_to_alias;
        NameToNameMap & array_join_alias_to_name = data.array_join_alias_to_name;
        NameToNameMap & array_join_result_to_source = data.array_join_result_to_source;

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
};

using ArrayJoinedColumnsVisitor = InDepthNodeVisitor<ArrayJoinedColumnsMatcher, true>;

}
