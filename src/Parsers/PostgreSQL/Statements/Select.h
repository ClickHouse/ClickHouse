#include "SelectSimple.h"

#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTExpressionList.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformSelectStatement(const std::shared_ptr<Node> node)
    {
        auto ast = std::make_shared<ASTSelectWithUnionQuery>();
        ast->list_of_selects = std::make_shared<ASTExpressionList>();
        if ((*node)["op"]->GetValueString() == "SETOP_NONE") {
            ast->list_of_selects->children.push_back(TransformSelectSimpleStatement(node));
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TransformSelectStatement not implemented");
        }
        ast->children.push_back(ast->list_of_selects);
        return ast;
    }


}
