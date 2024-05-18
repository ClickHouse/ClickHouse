#pragma once
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Parsers/PostgreSQL/Values/StringValue.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>


namespace DB::PostgreSQL
{
    ASTPtr TransformColumnType(const std::shared_ptr<Node> node)
    {
        const auto& typeNames = (*node)["names"]->GetNodeArray();
        if (typeNames.empty())
        {
            throw Exception(ErrorCodes::UNEXPECTED_AST, "Cannot find type names");
        }
        std::shared_ptr<Node> sValNode = typeNames[typeNames.size() - 1]->GetOnlyChild()->GetOnlyChild();
        if (!sValNode)
        {
            throw Exception(ErrorCodes::UNEXPECTED_AST, "Cannot find type name");
        }
        return TransformSVal(sValNode);
    }


}
