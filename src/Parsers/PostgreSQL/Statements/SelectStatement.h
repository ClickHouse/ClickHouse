#include <Parsers/ASTSelectWithUnionQuery.h>

namespace DB::PostgreSQL
{

    ASTPtr TransformSelectStatement(const std::shared_ptr<Node> /*node*/)
    {
        const auto ast = std::make_shared<ASTSelectWithUnionQuery>();
        return ast;
    }
}
