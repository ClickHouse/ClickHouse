#pragma once

#include <memory>
#include <functional>
#include <initializer_list>
#include <optional>
#include <string_view>
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

/** The write batch of an `OP_MSG` write command: the `documents` of an `insert`, the `deletes` of
  * a `delete` and the `updates` of an `update`. `OP_MSG` lets a client send it either inside the
  * command body as an array or as a separate kind-`1` document sequence section, and the section
  * is bound by its `identifier` - the name of that very field - rather than by its position in
  * the message. This reads both shapes: a document sequence whose identifier is `field_name` if
  * the message carries one, the array of the command body otherwise. A document sequence with
  * any other identifier is a controlled error rather than a payload read by position.
  */
std::vector<Document>
getWriteBatch(const std::vector<OpMessageSection> & sections, const char * field_name, const char * command);

/** Rejects a write command whose `ordered` flag is `false`. The `delete` and `update` handlers
  * apply their specs one after another and stop at the first error, and the `insert` handler
  * writes its documents all or nothing, i.e. they implement only the ordered semantics, the Mongo
  * default, or a stricter form of it. An unordered batch asks for the opposite - every spec is
  * attempted regardless of the errors of the others - so it is an error rather than being
  * silently executed as an ordered one.
  */
void rejectUnorderedWriteBatch(const Document & command, const char * command_name);

/** Rejects the options of a command (or of one spec of a write batch) that change its result in a
  * way the translation does not implement, e.g. a `collation` that makes `{"name": "foo"}` match
  * `"FOO"`, or the `arrayFilters` of an update. Executing such a command without the option would
  * answer a different question than the one asked, so it is an error instead. A `collation` with
  * the `simple` locale asks for the binary comparison, which is what the translation does, and
  * is accepted. An option that is absent or null is accepted.
  */
void rejectUnsupportedOptions(const rapidjson::Value & json, const char * command, std::initializer_list<std::string_view> options);

/** Rejects a `writeConcern` or a `readConcern` of a command that this server cannot honour. It is
  * a single server, the equivalent of a standalone `mongod`, which applies a write before it
  * replies. So it honours the write concerns `w: 1` and `w: "majority"` - the majority of one
  * server is the server itself - and `w: 0`, for which an acknowledged write is a stronger form.
  * It does not honour a write concern that waits for other members (a `w` above 1 or a tag set)
  * or for a flush to disk (`j: true`, `fsync: true`), because an insert is not synced to disk by
  * default. The read concern levels `local`, `available` and `majority` all mean the committed
  * data of a single server, which is what every query reads; `linearizable`, `snapshot` and the
  * cluster time options (`afterClusterTime`, `atClusterTime`) are not implemented. Executing a
  * command with a concern it cannot honour would answer with weaker guarantees than the ones the
  * client asked for, so it is an error instead. An absent or null concern is accepted.
  */
void rejectUnsupportedConcerns(const rapidjson::Value & json, const String & command);

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

/** The namespace of the Mongo dialect text that a handler builds for a command. The text never
  * holds the name of the collection itself: the parser cuts the namespace at the first `(` and
  * the statement at the first `;`, both of which a collection name may contain, e.g.
  * `sales(2026)`. The database and the collection are passed to `parseMongoQuery` explicitly and
  * override this placeholder.
  */
inline constexpr std::string_view MONGO_DIALECT_PLACEHOLDER_NAMESPACE = "db.collection";

/** Rejects a command that is a part of a multi-document transaction, i.e. one that carries
  * `startTransaction` or `autocommit`: transactions are not supported, so its writes would otherwise
  * be applied at once, while the client expects them to wait for the commit of the transaction.
  */
void rejectTransaction(const rapidjson::Value & json, const String & command);

/** Parses the Mongo dialect text that a handler builds for a command on `collection`, with the
  * parser limits of the session (`max_query_size`, `max_parser_depth`, `max_parser_backtracks`),
  * the same ones a query of the dialect sent over the other interfaces is parsed with.
  */
ASTPtr parseMongoDialectCommand(const String & mongo_dialect_query, const CollectionRef & collection, const QueryExecutor & executor);

/// Whether a database name is one `validateMongoDatabaseName` accepts.
bool isValidMongoDatabaseName(const String & database);

/** Checks a Mongo database name taken from the `$db` of a command. Every command validates it
  * the same way, so that the wire endpoint cannot create a namespace that a later command, such as
  * `dropDatabase`, or the dialect would reject.
  */
void validateMongoDatabaseName(const String & database, const String & command_name);

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
  * The `meta` of the result drives the conversion of each value (see `appendTypedValue`), and
  * the dotted name of a column - the way the dialect addresses a nested field - becomes the
  * nested document it names: the row `{"profile.name": "x"}` returns as `{"profile": {"name": "x"}}`.
  * `client_settings` are the settings of the client options of the command, such as `maxTimeMS`
  * (see `QueryExecutor::execute`).
  */
std::vector<Document> executeSelectIntoCursor(
    const String & sql_query,
    const CollectionRef & collection,
    std::shared_ptr<QueryExecutor> executor,
    const SettingsChanges & client_settings = {});

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

/** Whether the collection is the placeholder table that `createCollection` leaves behind: a
  * single `JSON` column named `json`, because an explicitly created collection has no document to
  * infer a schema from. The first `insert` gives it the schema of the inserted document.
  *
  * That column layout is also a perfectly ordinary table a user may have created directly in
  * ClickHouse, so the layout alone does not identify the placeholder: the table must additionally
  * carry the comment `createCollection` puts on it, see `PLACEHOLDER_COLLECTION_COMMENT`.
  */
bool isPlaceholderCollection(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor);

/** Whether the collection exists and has the columns of its documents, i.e. it is neither absent
  * nor the placeholder of `createCollection`. A collection that does not have them holds no
  * document, so the commands that read or modify documents treat it as empty: a filter over it
  * would otherwise name columns that do not exist yet and fail instead of matching nothing.
  */
bool collectionHasSchema(const CollectionRef & collection, std::shared_ptr<QueryExecutor> executor);

/** The `filter` of a `listCollections` / `listDatabases` command as a predicate over the names it
  * lists. The one field a filter can name here is `name`, matched by equality, `$eq`, `$in` or a
  * regular expression (`$regex` with `$options`, or `$regularExpression`). Any other shape is
  * rejected: a filter that is not understood must not widen into an unfiltered listing, which
  * would report names the client did not ask for. Without a `filter` every name passes.
  */
std::function<bool(const String &)> getNameFilter(const rapidjson::Value & command, const char * command_name);

/** A boolean option of a wire command, such as `nameOnly`. Returns nothing when the member is
  * absent or null; a value of another type is an error.
  */
std::optional<bool> getBoolOption(const rapidjson::Value & json, const char * name, const char * command);

/** The `maxTimeMS` of a read command - `find`, `count`, `distinct` and `aggregate` - as the settings
  * of the query that answers it: `max_execution_time` with `timeout_overflow_mode = 'throw'`. A
  * request with a deadline must not run without one. Nothing, i.e. the limits of the user's
  * profile, for an absent `maxTimeMS` and for `0`, which Mongo reads as no limit. The settings are
  * client settings of `QueryExecutor::execute`: they cannot loosen a tighter limit of the profile,
  * and they are subject to its constraints.
  */
SettingsChanges getMaxTimeSettings(const rapidjson::Value & json, const char * command);

/** Rejects a non-zero `maxTimeMS` of a write command - `delete` and `update`. Such a command runs
  * as a count of the matched documents followed by an awaited mutation, and a mutation is not
  * bounded by `max_execution_time`, so a deadline could not be honoured: a request with one must
  * not run without it.
  */
void rejectMaxTime(const rapidjson::Value & json, const char * command);

/** The number of rows a translated `find` returns, i.e. the number of documents a filter matches.
  * A mutation of ClickHouse is asynchronous and reports nothing about the rows it will rewrite,
  * so a write command counts the matched documents itself before submitting the mutation.
  */
Int64 countMatchedRows(const String & select_query, std::shared_ptr<QueryExecutor> executor);

/** The settings a write command submits its mutation with. A `delete` or an `update` carries a
  * list of specs that Mongo applies one after another, and the reply counts what each of them
  * matched, so a spec has to observe what the previous ones did. An `ALTER TABLE ... DELETE` is
  * asynchronous by default, which would let two overlapping specs count the same rows twice, so
  * every mutation is awaited before the next spec is counted.
  */
SettingsChanges getMutationSettings();

/// The comment `createCollection` puts on the placeholder table it creates, see `Create.cpp`.
extern const char * const PLACEHOLDER_COLLECTION_COMMENT;

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
