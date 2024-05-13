#pragma once
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformColumnType(const std::shared_ptr<Node> node)
    {
        auto ast = std::make_shared<ASTFunction>();
        const auto& typeNames = (*node)["names"]->GetNodeArray();
        for (const auto& name : typeNames) {
            const auto sValNode = name->GetOnlyChild()->GetOnlyChild();
        }
        return ast;
    }


}
