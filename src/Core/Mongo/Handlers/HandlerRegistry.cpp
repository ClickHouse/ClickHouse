#include <Core/Mongo/Handlers/HandlerRegistry.h>

namespace DB::MongoProtocol
{

HandlerRegitstry::HandlerRegitstry()
{
    registerFindHandler(this);
    registerInsertHandler(this);
    registerIsMasterHandler(this);
    registerDropHandler(this);
    registerCreateHandler(this);
    registerCountHandler(this);
    registerDeleteHandler(this);
    registerListDatabasesHandler(this);
    registerListCollectionsHandler(this);
    registerAuthHandler(this);
    registerUpdateHandler(this);
    registerIndexHandler(this);
}

void HandlerRegitstry::addHandler(const String & handler_name, HandlerPtr handler)
{
    hanlers[handler_name] = handler;
}

HandlerPtr HandlerRegitstry::getHandler(const String & handler_name) const
{
    if (!hanlers.contains(handler_name))
        return nullptr;
    return hanlers.at(handler_name);
}

}
