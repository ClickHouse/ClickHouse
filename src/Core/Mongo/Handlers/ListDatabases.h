#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct ListDatabasesHandler : IHandler
{
    ListDatabasesHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"listDatabases"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
