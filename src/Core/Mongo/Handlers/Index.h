#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct IndexHandler : IHandler
{
    IndexHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"createIndexes"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor) override;
};

}
