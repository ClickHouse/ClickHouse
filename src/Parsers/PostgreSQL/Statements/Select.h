#include "SelectSimple.h"

#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/SelectUnionMode.h>
#include <Parsers/ASTExpressionList.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformSelectStatement(const std::shared_ptr<Node> node)
    {
        auto ast = std::make_shared<ASTSelectWithUnionQuery>();
        ast->list_of_selects = std::make_shared<ASTExpressionList>();

        if ((*node)["all"]->GetBoolValue()) {
            ast->list_of_modes.push_back(SelectUnionMode::UNION_ALL);
            ast->set_of_modes.insert(SelectUnionMode::UNION_ALL);
        }

        if ((*node)["op"]->GetValueString() == "SETOP_NONE") 
        {
            ast->list_of_selects->children.push_back(TransformSelectSimpleStatement(node));
        }
        else if ((*node)["op"]->GetValueString() == "SETOP_UNION") 
        {
            if (!node->HasChildWithKey("larg") || !node->HasChildWithKey("rarg"))
            {
                throw Exception(ErrorCodes::UNEXPECTED_AST, "Expected two Select stmts");
            }
            ast->list_of_selects->children.push_back(TransformSelectSimpleStatement((*node)["larg"]));
            ast->list_of_selects->children.push_back(TransformSelectSimpleStatement((*node)["rarg"]));
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TransformSelectStatement not implemented");
        }
        ast->children.push_back(ast->list_of_selects);
        return ast;
    }


}
