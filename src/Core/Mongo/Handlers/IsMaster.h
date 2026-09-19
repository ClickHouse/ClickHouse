#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct IsMasterHandler : IHandler
{
    IsMasterHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"isMaster", "ismaster", "hello"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
