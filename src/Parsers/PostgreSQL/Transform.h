#include <Parsers/IAST_fwd.h>
#include <Parsers/PostgreSQL/Common/Types.h>

namespace DB::PostgreSQL
{
    void Transform(const JSON::Element& node, const ASTPtr & ast);
    void TransformStatement(const JSON::Object& node, const ASTPtr & ast);
}
