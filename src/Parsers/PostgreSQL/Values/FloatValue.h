#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformFVal(const std::shared_ptr<Node> node)
    {
        // somehow libpg_query parses float value to string (ExampleSelectFloat)
        const auto& fvalNode = (*node)["fval"];
        if (fvalNode->IsStringValue()) {
            Float64 val = std::stod(fvalNode->GetStringValue());
            return std::make_shared<ASTLiteral>(val);
        }
        else if(fvalNode->IsFloat64Value())
        {
            Float64 val = fvalNode->GetFloat64Value();
            return std::make_shared<ASTLiteral>(val);
        }
        throw Exception(ErrorCodes::UNEXPECTED_AST, "Expected fval to be String or Float64");
    }
}
