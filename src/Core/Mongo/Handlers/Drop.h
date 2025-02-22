#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct DropHandler : IHandler
{
    DropHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"drop"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session) override;
};

}
