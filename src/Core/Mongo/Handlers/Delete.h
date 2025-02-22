#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct DeleteHandler : IHandler
{
    DeleteHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"delete"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session) override;
};

}
