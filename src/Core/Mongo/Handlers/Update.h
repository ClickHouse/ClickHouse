#pragma once

#include <Core/Mongo/Handler.h>
#include "Core/Mongo/Document.h"

namespace DB::MongoProtocol
{

class UpdateHandler : public IHandler
{
public:
    UpdateHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"update"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor) override;
};

}
