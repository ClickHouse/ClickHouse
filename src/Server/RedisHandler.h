#pragma once

#include <memory>
#include <optional>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/TCPServerConnection.h>

#include <Core/ColumnWithTypeAndName.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IServer.h>
#include <Server/RedisProtocolMapping.h>
#include <Server/RedisProtocolRequest.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <base/types.h>
#include <Common/Logger.h>
#include <Common/QueryScope.h>

namespace DB
{

class ProcessListEntry;
class Session;
class TCPServer;

class RedisHandler : public Poco::Net::TCPServerConnection
{
public:
    RedisHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_);

    void run() final;

private:
    /// Interprets and executes an already read command.
    /// Returns false when the client has asked to close the connection.
    bool processRequest(RedisProtocol::RedisRequest & req);

    /// Authenticates the session with the given credentials. Throws on failure.
    void authenticate(const String & user_name, const String & password);

    /// Keeps a data command attached to a query: its thread group and its entry in the process list,
    /// which install the memory limits of the user and make the command visible in `system.processes`.
    /// Keep it alive for the whole command, and declare it before everything the command allocates.
    struct CommandScope
    {
        QueryScope query_scope;
        std::shared_ptr<ProcessListEntry> process_list_entry;
    };

    /// Data commands require an authenticated session. If the client has not sent AUTH,
    /// try to authenticate as the `default` user with an empty password; if that fails,
    /// the client has to authenticate explicitly.
    /// Then creates a fresh query context for the command and attaches the command to it.
    [[nodiscard]] CommandScope prepareQueryContext(const String & command_name);

    void checkDBSet() const;

    /// Reads the binding of the selected Redis database from the live server configuration. It is read
    /// for every request, so that `SYSTEM RELOAD CONFIG` is visible to already connected clients.
    RedisProtocol::MapDescription getMapDescription(UInt32 db_) const;

    /// A resolved table together with a share lock that prevents a concurrent DROP or DETACH
    /// from committing while a command reads the table. Keep it alive for the whole command.
    struct ResolvedTable
    {
        StoragePtr table;
        TableLockHolder lock;
    };

    /// Resolves the table configured for a Redis database, takes a share lock on it, and
    /// validates that it can serve lookups.
    /// The table is resolved for every request, so DDL on it is visible immediately.
    ResolvedTable resolveTable(UInt32 db_, const RedisProtocol::MapDescription & mapping) const;

    /// Checks that the table matches the configuration of the Redis database and supports lookups.
    void validateTable(UInt32 db_, const RedisProtocol::MapDescription & mapping, const StoragePtr & table) const;

    /// Renders the value in the given row of a lookup result column as a Redis bulk string.
    /// An unset optional means the key was not found (serialized as Nil).
    static std::optional<String> serializeValue(const ColumnWithTypeAndName & column, size_t row);

    IServer & server;
    TCPServer & tcp_server;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;
    std::unique_ptr<Session> session;
    ContextMutablePtr query_context;
    bool authenticated = false;

    UInt32 db = RedisProtocol::DB_MAX_NUM;

    LoggerPtr log = getLogger("RedisHandler");
};

}
