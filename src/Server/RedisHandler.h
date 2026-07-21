#pragma once

#include <map>
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
#include <base/types.h>
#include <Common/Logger.h>

namespace DB
{

class Session;
class TCPServer;

class RedisHandler : public Poco::Net::TCPServerConnection
{
public:
    RedisHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, RedisProtocol::ConfigPtr config_);

    void run() final;

private:
    /// Returns false when the client has asked to close the connection.
    bool processRequest();

    /// Authenticates the session with the given credentials. Throws on failure.
    void authenticate(const String & user_name, const String & password);

    /// Data commands require an authenticated session. If the client has not sent AUTH,
    /// try to authenticate as the `default` user with an empty password; if that fails,
    /// the client has to authenticate explicitly.
    void ensureAuthenticated();

    void initDB(UInt32 db_);
    void checkDBSet() const;

    /// Renders a single value of a single-row lookup result as a Redis bulk string.
    /// An unset optional means the key was not found (serialized as Nil).
    static std::optional<String> serializeValue(const ColumnWithTypeAndName & column);

    IServer & server;
    TCPServer & tcp_server;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;
    std::unique_ptr<Session> session;
    ContextMutablePtr query_context;
    bool authenticated = false;

    RedisProtocol::ConfigPtr config;
    UInt32 db = RedisProtocol::DB_MAX_NUM;
    std::map<UInt32, RedisProtocol::MappingPtr> redis_clickhouse_mapping;

    LoggerPtr log = getLogger("RedisHandler");
};

}
