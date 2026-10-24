#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct AuthHandler : IHandler
{
    AuthHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"saslStart"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
