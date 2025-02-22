#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct CreateHandler : IHandler
{
    CreateHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"create"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session) override;
};

}
