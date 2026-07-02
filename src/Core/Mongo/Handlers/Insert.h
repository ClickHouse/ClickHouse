#pragma once

#include <Core/Mongo/Handler.h>
#include "Core/Mongo/Document.h"

namespace DB::MongoProtocol
{

class InsertHandler : public IHandler
{
public:
    struct DocumnetField
    {
        String full_name;
        String type;
    };

    InsertHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"insert"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;

private:
    void createDatabase(const Document & doc, std::shared_ptr<QueryExecutor> executor);
    String createTable(const Document & doc, std::shared_ptr<QueryExecutor> executor, const std::vector<DocumnetField> & types);
};

}
