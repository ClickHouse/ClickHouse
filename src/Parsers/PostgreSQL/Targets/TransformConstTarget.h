#include <Parsers/PostgreSQL/Values/IntValue.h>
#include <Parsers/PostgreSQL/Values/BoolValue.h>
#include <Parsers/PostgreSQL/Values/FloatValue.h>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Common/Exception.h>

#include <Parsers/IAST_fwd.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformConstTarget(const std::shared_ptr<Node> node)
    {
        if (node->HasChildWithKey("ival"))
        {
            return TransformIVal((*node)["ival"]);
        }
        else if (node->HasChildWithKey("fval"))
        {
            return TransformFVal((*node)["fval"]);
        }
        else if (node->HasChildWithKey("boolval"))
        {
            return TransformBoolVal((*node)["boolval"]);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
        }
    }
}
