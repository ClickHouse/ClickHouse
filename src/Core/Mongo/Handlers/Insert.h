#pragma once

#include <Core/Mongo/Document.h>
#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

class InsertHandler : public IHandler
{
public:
    InsertHandler() = default;

    std::vector<String> getIdentifiers() const override { return {"insert"}; }

    std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) override;

private:
    void createDatabase(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor);

    /** Creates a collection of documents: one `JSON` column holding the document of each row and an
      * `_id` column holding its object id, which is the primary key. A Mongo collection has no
      * schema, so there is nothing to infer from the first document - and nothing that a later one
      * can contradict.
      */
    void createCollection(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor);
};

}
