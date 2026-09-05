#include <Core/Mongo/DocumentCollectionShape.h>

#include <Parsers/Mongo/DocumentCollection.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>

#include <fmt/format.h>
#include <Common/quoteString.h>

namespace DB::MongoProtocol
{

CollectionShape getCollectionShape(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor)
{
    /** A collection of documents has the `json` column and nothing but the object id next to it.
      * Its explicit table comment distinguishes it from a ClickHouse table that merely happens to
      * have this same schema.
      *
      * The comment is read by a subquery of its own rather than by a join with `system.tables`:
      * `system.columns` has a `comment` column as well - the comment of the column - so a name
      * that is not qualified reads that one and never matches.
      */
    auto answer = executor->execute(fmt::format(
        "SELECT countIf(name = {} AND type = 'JSON'), countIf(name = {}), count(), "
        "(SELECT count() FROM system.tables WHERE database = {} AND table = {} AND comment = {}) "
        "FROM system.columns WHERE database = {} AND table = {} FORMAT TSV",
        quoteString(String(Mongo::DOCUMENT_COLUMN)),
        quoteString(String(Mongo::OBJECT_ID_COLUMN)),
        quoteString(collection.database),
        quoteString(collection.collection),
        quoteString(String(Mongo::DOCUMENT_COLLECTION_COMMENT)),
        quoteString(collection.database),
        quoteString(collection.collection)));

    auto fields = splitByNewline(answer);
    if (fields.empty())
        return {};

    UInt64 document_columns = 0;
    UInt64 object_id_columns = 0;
    UInt64 columns = 0;
    UInt64 document_collection = 0;
    ReadBufferFromString buffer(fields.front());
    if (!tryReadText(document_columns, buffer) || !checkChar('\t', buffer) || !tryReadText(object_id_columns, buffer)
        || !checkChar('\t', buffer) || !tryReadText(columns, buffer) || !checkChar('\t', buffer) || !tryReadText(document_collection, buffer))
        return {};

    CollectionShape shape;
    shape.exists = columns > 0;
    shape.has_object_id = object_id_columns == 1;
    shape.stores_documents = document_collection == 1 && document_columns == 1 && columns == 1 + object_id_columns;
    return shape;
}

bool adaptQueryToCollectionShape(
    const ASTPtr & query, const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor, bool reads_whole_documents)
{
    const auto shape = getCollectionShape(collection, executor);
    if (!shape.stores_documents)
        return false;

    Mongo::rewriteFieldsAsDocumentPaths(query);
    return reads_whole_documents && Mongo::selectDocumentsOfCollection(query);
}

}
