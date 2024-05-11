#include <Parsers/PostgreSQL/Values/IValue.h>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Common/Exception.h>

#include <Parsers/IAST_fwd.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformConstTarget(const std::shared_ptr<Node> node)
    {
        std::cerr << "TransformConstTarget\n";
        if (node->HasChildWithKey("ival"))
        {
            return TransformIVal((*node)["ival"]);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
        }
    }
}
