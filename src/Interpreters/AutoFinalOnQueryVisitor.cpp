#include "AutoFinalOnQueryVisitor.h"
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>

namespace DB
{

void AutoFinalOnQuery::visit(ASTPtr & query, Data & data)
{
    if (auto * select_query = query->as<ASTSelectQuery>())
        visit(*select_query, data.storage, data.context);
}

void AutoFinalOnQuery::visit(ASTSelectQuery & query, StoragePtr storage, ContextPtr context)
{
    if (autoFinalOnQuery(query, storage, context))
    {
        query.setFinal();
    }
}

bool AutoFinalOnQuery::needChildVisit(ASTPtr &, const ASTPtr &)
{
    return true;
}

bool AutoFinalOnQuery::autoFinalOnQuery(ASTSelectQuery & query, StoragePtr storage, ContextPtr context)
{
    // query.tables() is required because not all queries have tables in it, it could be a function.
    bool is_auto_final_setting_on = context->getSettingsRef().final;
    bool is_final_supported = storage && storage->supportsFinal() && !storage->isRemote() && query.tables();
    bool is_query_already_final = query.final();

    return is_auto_final_setting_on && !is_query_already_final && is_final_supported;
}

}
