#include <Storages/SelectQueryInfo.h>
#include <Parsers/ASTSelectQuery.h>

namespace DB
{

bool SelectQueryInfo::isFinal() const
{
    if (table_expression_modifiers)
        return table_expression_modifiers->hasFinal();

    const auto & select = query->as<ASTSelectQuery &>();
    return select.final();
}

}
