#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTLiteral.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformBoolVal(const std::shared_ptr<Node> node)
    {
        bool val = (*node)["boolval"]->GetBoolValue();
        return std::make_shared<ASTLiteral>(val);
    }
}
