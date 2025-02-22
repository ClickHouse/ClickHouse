#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct ListCollectionsHandler : IHandler
{
    ListCollectionsHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"listCollections"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session) override;
};

}
