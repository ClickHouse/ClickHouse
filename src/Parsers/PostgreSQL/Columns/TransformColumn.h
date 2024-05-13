#pragma once
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
        TransformColumnType((*node)["TypeName"]);

        return ast;
    }


}
