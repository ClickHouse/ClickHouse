#pragma once
#include <memory>
#include "TransformColumnType.h"

#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTColumnDeclaration.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformColumn(const std::shared_ptr<Node> node)
    {
        auto ast = std::make_shared<ASTColumnDeclaration>();
        ast->name = (*node)["colname"]->GetStringValue();
        ast->type = TransformColumnType((*node)["typeName"]);

        return ast;
    }


}
