#pragma once
#include <Parsers/PostgreSQL/Statements/Select.h>
#include <Parsers/PostgreSQL/Statements/Create.h>

namespace DB::PostgreSQL
{
    ASTPtr TransformStatement(const std::shared_ptr<Node> node)
    {
        if (node->GetKeyString() == "SelectStmt")
        {
            return TransformSelectStatement(node);
        }
        else if (node->GetKeyString() == "CreateStmt")
        {
            return TransformCreateStatement(node);
        }
        else
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
        }
    }
}
