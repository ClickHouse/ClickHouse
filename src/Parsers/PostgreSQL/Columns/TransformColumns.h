#pragma once
#include "TransformColumn.h"

#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformColumns(const std::shared_ptr<Node> node)
    {
        auto ast = std::make_shared<ASTColumns>();
        auto columns_exp_list = std::make_shared<ASTExpressionList>();
        
        auto columns_nodes = node->GetNodeArray();
        for (const auto& column_node : columns_nodes) {
            auto column = TransformColumn(column_node->GetOnlyChild());
            columns_exp_list->children.push_back(column);
        }

        ast->set(ast->columns, columns_exp_list);
        return ast;
    }

}
