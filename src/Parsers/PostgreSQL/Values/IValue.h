#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformIVal(const std::shared_ptr<Node> node)
    {
        Int64 val = (*node)["ival"]->GetInt64Value();
        return std::make_shared<ASTLiteral>(val);
    }
}
