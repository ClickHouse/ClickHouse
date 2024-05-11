#include "Transform.h"
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Parsers/PostgreSQL/Statements/SelectStatement.h>
#include <Common/Exception.h>

namespace DB::PostgreSQL
{
    ASTPtr Transform(const std::shared_ptr<Node> node) 
    {
        if (node->GetKeyString() != "stmts") {
            throw Exception(ErrorCodes::UNEXPECTED_AST, "root node should have stmts key");
        }
        const auto& stmts = (*node)["stmts"]->GetNodeArray();
        if (stmts.size() != 1)
        {
            throw Exception(ErrorCodes::UNEXPECTED_AST, "Expected one statement");
        }
        return TransformStatement(stmts[0]);
    }

    ASTPtr TransformStatement(const std::shared_ptr<Node> node)
    {
        if (node->GetKeyString() == "SelectStmt")
        {
            return TransformSelectStatement(node);
        } 
        else
        {
            throw Exception(ErrorCodes::UNEXPECTED_AST, "Expected statement type");
        }
    }
}
