#pragma once

#include <memory>
#include <Core/Mongo/Document.h>
#include <Core/Mongo/MongoProtocol.h>
#include <Core/Mongo/Wire/OpMessage.h>
#include <Core/Mongo/Wire/OpQuery.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>

namespace DB::MongoProtocol
{

std::vector<std::string> splitByNewline(const std::string & s);

String modifyFilter(const String & json);

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
