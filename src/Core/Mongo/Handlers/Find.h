#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct FindHandler : IHandler
{
    FindHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"find"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
