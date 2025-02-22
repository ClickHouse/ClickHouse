#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct CountHandler : IHandler
{
    CountHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"count"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session) override;
};

}
