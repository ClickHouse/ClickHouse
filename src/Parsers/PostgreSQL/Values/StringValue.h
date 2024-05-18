#include <memory>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>
#include "base/types.h"
#include <Parsers/ASTFunction.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformSVal(const std::shared_ptr<Node> node)
    {
        String val = node->GetStringValue();
        if (val == "int32") {
            return makeASTFunction("Int32");
        } else if (val == "int64")
        {
            return makeASTFunction("Int64");
        } else if (val == "uint32")
        {
            return makeASTFunction("Uint32");
        } else if (val == "uint64")
        {
            return makeASTFunction("Uint64");
        } else if (val == "string")
        {
            return makeASTFunction("String");
        } else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unknown sval: {}", val);
        }
    }
}
