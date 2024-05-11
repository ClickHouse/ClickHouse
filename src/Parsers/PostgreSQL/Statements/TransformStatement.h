#include <Parsers/PostgreSQL/Statements/Select.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformStatement(const std::shared_ptr<Node> node)
    {
        if (node->GetKeyString() == "SelectStmt")
        {
            return TransformSelectStatement(node);
        } 
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
        }
    }
}
