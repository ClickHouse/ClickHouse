#pragma once
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/PostgreSQL/Columns/TransformColumns.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTExpressionList.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformCreateStatement(const std::shared_ptr<Node> node)
    {
        auto ast = std::make_shared<ASTCreateQuery>();
        if (node->HasChildWithKey("tableElts"))
        {
            ast->set(ast->columns_list, TransformColumns((*node)["tableElts"]));
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
        }

        return ast;
    }

}
