#include "Transform.h"
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Parsers/PostgreSQL/Statements/SelectStatement.h>
#include <Common/Exception.h>

namespace DB::PostgreSQL
{
    // void Transform(const JSON::Element& node, const ASTPtr & ast) 
    // {
    //     if (!node.isObject()) {
    //         throw Exception(ErrorCodes::UNEXPECTED_AST, "Root should be RapidJson::Object type");
    //     }
    //     const auto& obj = node.getObject();
    //     if (obj.size() != 1) {
    //         throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    //     }

    //     JSON::Array stmts = FindArrayChild(obj, "stmts");
    //     if (stmts.size() != 1) {
    //         throw Exception(ErrorCodes::NOT_IMPLEMENTED, "No support for multiple statements");
    //     }

    //     JSON::Object stmt = FindObjectByIndex(stmts, 0);
    //     TransformStatement(stmt, ast);
    // }

    // void TransformStatement(const JSON::Object& node, const ASTPtr & ast)
    // {
    //     if (node.size() != 1) {
    //         throw Exception(ErrorCodes::UNEXPECTED_AST, "stmnt should have exactly one child");
    //     }
    // }
}
