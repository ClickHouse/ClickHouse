#include "Utils.h"
#include <Core/Field.h>

namespace DB
{

[[maybe_unused]] bool isNumeric(DataTypePtr)
{
    /// TODO
    return true;
}

bool isConstColumn(const ActionsDAG::Node * node_)
{
    return node_->column && isColumnConst(*node_->column);
}

bool isAlwaysFalse(const ASTPtr & ast)
{
    if (auto literal = ast->as<ASTLiteral>())
    {
        if (isInt64OrUInt64orBoolFieldType(literal->value.getType()))
            return literal->value.safeGet<UInt64>() == 0;
        if (literal->value.getType() == Field::Types::Bool)
            return !literal->value.safeGet<bool>();
    }
    return false;
}

}
