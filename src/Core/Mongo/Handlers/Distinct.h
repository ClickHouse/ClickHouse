#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct DistinctHandler : IHandler
{
    DistinctHandler() = default;

    [[nodiscard]] std::vector<String> getIdentifiers() const override { return {"distinct"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
