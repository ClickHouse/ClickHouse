#pragma once

#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

class InsertHandler : public IHandler
{
public:
    /// A single ClickHouse column inferred from a Mongo document. Nested documents are
    /// flattened, so `full_name` may be a dot separated path such as `address.city`.
    struct DocumentField
    {
        String full_name;
        String type;
    };

    InsertHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"insert"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;

private:
    void createDatabase(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor);
    void createTable(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor, const std::vector<DocumentField> & fields);
};

}
