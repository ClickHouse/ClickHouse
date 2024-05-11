#include <Parsers/PostgreSQL/Statements/Select.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformStatement(const std::shared_ptr<Node> node)
    {
        std::cerr << "TransformStatement\n";
        std::cerr << "GetKeyString: " << node->GetKeyString() << "\n";
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
