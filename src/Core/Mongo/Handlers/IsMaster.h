#pragma once

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

struct IsMasterHandler : IHandler
{
    IsMasterHandler() = default;

    /// `hello` is the name the handshake has had since MongoDB 5.0, and the drivers send it; the
    /// two spellings of `isMaster` are the older name of one and the same command.
    std::vector<String> getIdentifiers() const override { return {"hello", "isMaster", "ismaster"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;
};

}
