#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct AggregateHandler : IHandler
{
    AggregateHandler() = default;

    [[nodiscard]] std::vector<String> getIdentifiers() const override { return {"aggregate"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
