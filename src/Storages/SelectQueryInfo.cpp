#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

bool SelectQueryInfo::isFinal() const
{
    if (table_expression_modifiers)
        return table_expression_modifiers->hasFinal();

    const auto & select = query->as<ASTSelectQuery &>();
    return select.final();
}


bool SelectQueryInfo::isStream() const
{
    return table_expression_modifiers && table_expression_modifiers->hasStream();
}

}
