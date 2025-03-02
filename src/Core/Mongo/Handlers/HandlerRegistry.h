#pragma once

#include <unordered_map>
#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

class HandlerRegitstry
{
public:
    HandlerRegitstry();

    void addHandler(const String & handler_name, HandlerPtr handler);

    HandlerPtr getHandler(const String & handler_name) const;

private:
    std::unordered_map<String, HandlerPtr> hanlers;
};

void registerFindHandler(HandlerRegitstry * registry);
void registerInsertHandler(HandlerRegitstry * registry);
void registerIsMasterHandler(HandlerRegitstry * registry);
void registerDropHandler(HandlerRegitstry * registry);
void registerCreateHandler(HandlerRegitstry * registry);
void registerCountHandler(HandlerRegitstry * registry);
void registerDeleteHandler(HandlerRegitstry * registry);
void registerListDatabasesHandler(HandlerRegitstry * registry);
void registerListCollectionsHandler(HandlerRegitstry * registry);
void registerAuthHandler(HandlerRegitstry * registry);
void registerUpdateHandler(HandlerRegitstry * registry);
void registerIndexHandler(HandlerRegitstry * registry);

}
