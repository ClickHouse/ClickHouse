#include "Transform.h"
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Parsers/PostgreSQL/Statements/SelectStatement.h>
#include <Common/Exception.h>

namespace DB::PostgreSQL
{
    // void Transform(const std::shared_ptr<Node> node, const ASTPtr & ast) 
    // {
    //     if (node->GetKeyString() != "stmts") {
    //         throw Exception(ErrorCodes::UNEXPECTED_AST, "root node should have stmts key");
    //     }
    //     const auto& child = (*node)["stmts"];
    //     
    // }

    // void TransformStatement(const JSON::Object& node, const ASTPtr & ast)
    // {
    //     if (node.size() != 1) {
    //         throw Exception(ErrorCodes::UNEXPECTED_AST, "stmnt should have exactly one child");
    //     }
    // }
}
