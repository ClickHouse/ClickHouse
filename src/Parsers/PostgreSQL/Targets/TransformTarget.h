#include "TransformResTarget.h"

#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>
#include <Parsers/PostgreSQL/Common/Errors.h>
#include <Common/Exception.h>

#include <Parsers/IAST_fwd.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformTarget(const std::shared_ptr<Node> node)
    {
        if (node->GetKey() == "ResTarget")
        {
            return TransformResTarget(node);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "TransformTarget not implemented");
        }
    }
}
