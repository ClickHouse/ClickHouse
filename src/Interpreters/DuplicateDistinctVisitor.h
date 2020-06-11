#pragma once

#include <Functions/FunctionFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Common/typeid_cast.h>

namespace DB
{

/// Removes duplicate DISTINCT from queries.
class DuplicateDistinctMatcher
{
public:
    struct Data
    {
        bool is_distinct;
        std::vector<String> last_ids;
    };

    static void visit(const ASTPtr & ast, Data & data)
    {
        auto * select_query = ast->as<ASTSelectQuery>();
        if (select_query)
            visit(*select_query, data);
    }

    static void visit(ASTSelectQuery & select_query, Data & data)
    {
        if (!select_query.distinct || !select_query.select())
            return;

        /// Optimize shouldn't work for distributed tables
        for (const auto & elem : select_query.children)
        {
            if (elem->as<ASTSetQuery>() && !elem->as<ASTSetQuery>()->is_standalone)
                return;
        }

        auto expression_list = select_query.select();
        std::vector<String> current_ids;

        if (expression_list->children.empty())
            return;

        current_ids.reserve(expression_list->children.size());
        for (const auto & id : expression_list->children)
            current_ids.push_back(id->getColumnName());

        if (data.is_distinct && current_ids == data.last_ids)
            select_query.distinct = false;

        data.is_distinct = true;
        data.last_ids = std::move(current_ids);
    }

    static bool needChildVisit(const ASTPtr &, const ASTPtr &)
    {
        return true;
    }

};

using DuplicateDistinctVisitor = InDepthNodeVisitor<DuplicateDistinctMatcher, false>;

}
