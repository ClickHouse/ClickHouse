#include "SelectSimple.h"

#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformSelectStatement(const std::shared_ptr<Node> node)
    {
        PrintDebugInfo(node);
        auto ast = std::make_shared<ASTSelectWithUnionQuery>();
        auto exprList = std::make_shared<ASTExpressionList>();
        if ((*node)["op"]->GetValueString() == "SETOP_NONE") {
            auto simpleSelect = TransformSelectSimpleStatement(node);
        }
        ast->children.push_back(exprList);
        return ast;
    }


}
