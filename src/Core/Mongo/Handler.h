#pragma once

#include <memory>
#include <optional>
#include <unordered_set>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>

namespace DB::MongoProtocol
{

std::vector<std::string> splitByNewline(const std::string & s);

String modifyFilter(const String & json);

/// Serializes an aggregation pipeline, normalizing the filter of every `$match` stage the same
/// way the filter of a `find` is normalized (see `modifyFilter`): `$match` uses the query syntax,
/// where a nested document names nested fields rather than holding a value.
String serializePipeline(const rapidjson::Value & pipeline);

/** A numeric option of a wire command that must be a whole number, such as the `limit` and the
  * `skip` of `find` and `count`. Drivers and the shell send whole numbers as BSON doubles, so an
  * integral double is accepted; a fractional one is an error rather than a silent truncation to
  * a bound the client never asked for. Returns nothing when the member is absent or null.
  */
std::optional<Int64> getWholeNumberOption(const rapidjson::Value & json, const char * name, const char * command);

/** Refuses a write concern the endpoint cannot honour. A write goes to one ClickHouse table and is
  * acknowledged when it is written, which is what `{w: 1}` and, on a server that is not a replica
  * set, `{w: "majority"}` ask for. Anything else - more than one acknowledgement, a tag set, or a
  * commit to the journal - is refused rather than answered with `ok` for a weaker write than the
  * client asked for.
  */
void validateWriteConcern(const rapidjson::Value & json, const char * command);

/** Refuses a read concern the endpoint cannot honour. A read goes to one ClickHouse table and sees
  * everything written to it, which is what `local`, `available` and, on a server that is not a
  * replica set, `majority` ask for. A snapshot, a linearizable read, or a read pinned to a cluster
  * time is refused rather than answered with a plain read.
  */
void validateReadConcern(const rapidjson::Value & json, const char * command);

/** Refuses the top-level fields of a read command that this endpoint does not implement. A read
  * command carries the fields a driver attaches to every command - the target database, the
  * session, the read preference, the version of the API - and those are accepted; a field that
  * changes what the command answers, such as a `collation` or a `batchSize`, would otherwise be
  * dropped silently and the command would answer a different query than the one it was sent.
  */
void rejectUnsupportedCommandFields(
    const rapidjson::Value & json, const std::unordered_set<String> & supported, const char * command);

/** Refuses a document-valued option of a command unless it asks for nothing: absent, null, or an
  * empty document. A non-empty one - the `filter` of a `listCollections`, the `cursor` options of
  * a command answered whole in the first batch - would change the answer, so it is refused rather
  * than dropped.
  */
void rejectNonEmptyDocumentOption(const rapidjson::Value & json, const char * name, const char * command);

/** The `filter` of a `listCollections` or `listDatabases` command, reduced to what is
  * implemented: no filter at all, or an equality on `name` - which is what a driver itself sends
  * when it probes for one collection, e.g. the `create_collection` of PyMongo lists with
  * `{"name": <name>}` first. Returns the name to keep, or nothing when every name passes; any
  * other filter is refused rather than ignored.
  */
std::optional<String> getNameEqualityFilter(const rapidjson::Value & json, const char * command);

/// A boolean option of a command, such as the `nameOnly` of `listCollections` and `listDatabases`.
/// Returns nothing when the member is absent or null; anything but a boolean is an error.
std::optional<bool> getBoolOption(const rapidjson::Value & json, const char * name, const char * command);

/** Refuses the fields of a command, or of one of the write statements of a command, that this
  * endpoint does not implement. A field that changes what a write does - `arrayFilters` and
  * `collation` of an `update`, `collation` of a `delete` - would otherwise be dropped silently and
  * the command would answer `ok` for a different write than the one it was asked for.
  */
void rejectUnsupportedFields(
    const rapidjson::Value & json, const std::unordered_set<String> & supported, const char * what, const char * command);

/** The target of a Mongo command: the collection named by the command field itself and the
  * database taken from the `$db` field of the command document. Mongo databases are mapped
  * to ClickHouse databases, so collections with the same name in different Mongo databases
  * are distinct tables.
  */
struct CollectionRef
{
    String database;
    String collection;

    /// A quoted `database`.`collection` identifier that is safe to embed into a query.
    String getQualifiedName() const;
};

/** Extracts the target of a command, e.g. for `{"find": "users", "$db": "app"}` and
  * `command_name` = `find` it returns the collection `users` of the database `app`.
  */
CollectionRef getCollectionRef(const Document & command, const String & command_name);

/** Appends one value of a `FORMAT JSON` result to a BSON document under `key`, using the
  * ClickHouse type of its column to restore the BSON type the JSON text does not carry:
  * a `DateTime`/`DateTime64`/`Date` becomes a BSON date rather than a string, an integer
  * column an `int32`/`int64` of its width, a `Bool` a boolean, and an `Array`/`Tuple`/`Map`
  * recurses element by element. A value whose column type carries no more information than
  * the JSON itself (`JSON`, `Dynamic`) is converted structurally.
  */
void appendTypedValue(bson_t * document, const String & key, const rapidjson::Value & value, const DataTypePtr & type);

/// The names and types of the columns of a parsed `FORMAT JSON` result, from its `meta`.
std::vector<std::pair<String, DataTypePtr>> extractResultColumns(const rapidjson::Document & result_json);

/** Runs a `SELECT` and builds the reply a Mongo client expects from a command that returns
  * documents: `{"cursor": {"firstBatch": [...], "id": 0, "ns": "<database>.<collection>"}, "ok": 1}`.
  * The whole result is returned in the first batch, so the cursor is already exhausted.
  * `sql_query` must carry no `FORMAT` clause: the function appends a row-per-line format of its
  * own and streams the rows into the reply as they arrive, checking the reply against
  * `maxBsonObjectSize` on every row, so an oversized result cancels the query rather than being
  * materialized first. The names-and-types header of the result drives the conversion of each
  * value (see `appendTypedValue`), and the dotted name of a column - the way the dialect
  * addresses a nested field - becomes the nested document it names: the row
  * `{"profile.name": "x"}` returns as `{"profile": {"name": "x"}}`.
  *
  * `holds_documents` says that the query was rewritten to return the documents of a collection as
  * they are stored, so that each row is turned into a reply out of its document rather than out of
  * the columns of the result. It is what `adaptQueryToCollectionShape` answered, rather than
  * anything read off the result: a column name is part of no contract.
  */
std::vector<Document> executeSelectIntoCursor(
    const String & sql_query,
    const CollectionRef & collection,
    std::shared_ptr<QueryExecutor> executor,
    bool holds_documents = false);

/** The reply of a document-returning command whose result is empty: a cursor with no rows in
  * its first batch. Mongo reads a collection that does not exist as empty rather than raising
  * an error, so the read commands reply with this when the table is absent.
  */
std::vector<Document> makeEmptyCursorReply(const CollectionRef & collection);

/** Whether an object exists, e.g. `object_kind` = `TABLE` and `name` = `` `db`.`users` ``.
  * Mongo treats operations on a namespace that does not exist as a no-op rather than an error,
  * so the handlers have to be able to tell it apart.
  */
bool objectExists(std::shared_ptr<QueryExecutor> executor, const String & object_kind, const String & name);

struct IHandler
{
    virtual std::vector<String> getIdentifiers() const = 0;
    virtual std::vector<Document> handle(const std::vector<OpMessageSection> & documents, std::shared_ptr<QueryExecutor> executor) = 0;

    virtual ~IHandler() = default;
};
using HandlerPtr = std::shared_ptr<IHandler>;

Header makeResponseHeader(Header request_header, Int32 message_size, Int32 response_id);

std::vector<Document> runMessageRequest(const std::vector<OpMessageSection> & sections, std::shared_ptr<QueryExecutor> executor);
std::vector<Document> runQueryRequst(const std::vector<Document> & documents, std::shared_ptr<QueryExecutor> executor);

/** Handles a single message. `payload` must contain exactly the bytes of that message
  * following its header, so that message boundaries are respected.
  */
void handle(
    const Header & header, ReadBuffer & payload, std::shared_ptr<MessageTransport> transport, std::shared_ptr<QueryExecutor> executor);

}
