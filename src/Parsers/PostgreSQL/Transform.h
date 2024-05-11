#include <Parsers/IAST_fwd.h>
#include <Parsers/PostgreSQL/Common/Types.h>
#include <Parsers/PostgreSQL/Common/util/JSONHelpers.h>

namespace DB::PostgreSQL
{
    ASTPtr Transform(const std::shared_ptr<Node> node);
    ASTPtr TransformStatement(const std::shared_ptr<Node>);
}
