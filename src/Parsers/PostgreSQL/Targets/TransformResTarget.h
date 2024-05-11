#include "TransformConstTarget.h"

#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Common/Exception.h>

#include <Parsers/IAST_fwd.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformResTarget(const std::shared_ptr<Node> node)
    {
        auto val = (*node)["val"];
        if (val->HasChildWithKey("A_Const"))
        {
            return TransformConstTarget(val->GetOnlyChild());
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TransformResTarget not implemented");
        }
    }
}
