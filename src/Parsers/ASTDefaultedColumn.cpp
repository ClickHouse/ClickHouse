#include <Parsers/ASTDefaultedColumn.h>

namespace DB
{

ASTPtr ASTDefaultedColumn::clone() const
{
    const auto res = std::make_shared<ASTDefaultedColumn>(*this);
    res->name->clone();
    res->expression->clone();
    return res;
}
}