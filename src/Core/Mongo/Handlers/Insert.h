#pragma once

#include <Core/Mongo/Handler.h>
#include "Core/Mongo/Document.h"

namespace DB::MongoProtocol
{

class InsertHandler : public IHandler
{
public:
    InsertHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"insert"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::unique_ptr<Session> & session) override;

private:
    void createDatabase(const Document & doc, std::unique_ptr<Session> & session);
    String createTable(const Document & doc, std::unique_ptr<Session> & session);
};

}
