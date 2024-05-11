#include "Transform.h"
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Parsers/PostgreSQL/Statements/TransformStatement.h>
#include <Common/Exception.h>

namespace DB::PostgreSQL
{
    ASTPtr Transform(const std::shared_ptr<Node> node) 
    {
        if (!node->HasChildWithKey("stmts")) {
            throw Exception(DB::ErrorCodes::UNEXPECTED_AST, "root node should have child with stmts key");
        }
        const auto& stmts = (*node)["stmts"]->GetNodeArray();
        if (stmts.size() != 1)
        {
            throw Exception(DB::ErrorCodes::UNEXPECTED_AST, "Expected exactly one statement");
        }
        const auto& stmt = stmts[0];
        return TransformStatement((*stmt)["stmt"]->GetOnlyChild());
    }
}
