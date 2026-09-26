#pragma once

#include <memory>

#include <Core/Mongo/Handler.h>

namespace DB::MongoProtocol
{

/** How a collection keeps its documents.
  *
  * A collection the Mongo endpoint creates keeps whole documents in one `JSON` column named `json`,
  * with the object id of each of them in an `_id` column, because a Mongo collection has no schema.
  * A table that was created in ClickHouse keeps its own columns, and a field of a query names the
  * column of the same name there. The endpoint marks its collections in table metadata, so a
  * ClickHouse table with otherwise identical columns remains a schemaful table.
  */
struct CollectionShape
{
    /// The collection keeps whole documents in the `json` column.
    bool stores_documents = false;

    /// The collection has an `_id` column, which holds the object id of the document.
    bool has_object_id = false;

    /// Whether the collection exists at all. A collection that does not exist is read as empty
    /// rather than by a shape of its own.
    bool exists = false;
};

/// The shape of a collection that exists. A table that does not exist has neither of the columns.
CollectionShape getCollectionShape(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor);

/** Adapts a query translated from Mongo to the shape of the collection it reads, and answers whether
  * the result of it holds whole stored documents. A query over a collection of documents addresses
  * every field as a path of the document column; over a table with columns of its own it is left as
  * it is, because a field names the column of the same name there.
  *
  * `reads_whole_documents` says that the reply is the documents of the collection - which is what a
  * `find` with no projection asks for - so that they are selected as they are stored, with their
  * object ids and the types of their paths.
  */
bool adaptQueryToCollectionShape(
    const ASTPtr & query,
    const CollectionRef & collection,
    std::shared_ptr<QueryExecutor> executor,
    bool reads_whole_documents = false);

}
