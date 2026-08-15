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
      */
    auto answer = executor->execute(fmt::format(
        "SELECT countIf(name = {} AND type = 'JSON'), countIf(name = {}), count(), any(comment = {}) FROM system.columns "
        "LEFT JOIN system.tables USING (database, table) "
        "WHERE database = {} AND table = {} FORMAT TSV",
        quoteString(String(Mongo::DOCUMENT_COLUMN)),
        quoteString(String(Mongo::OBJECT_ID_COLUMN)),
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
    if (reads_whole_documents)
        Mongo::selectDocumentsOfCollection(query);
    return true;
}

}
