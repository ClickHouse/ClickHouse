#include <Server/MongoHandler.h>

/// The Mongo wire protocol needs BSON (mongo-cxx-driver) and the Mongo dialect (rapidjson).
#if USE_MONGODB && USE_RAPIDJSON

#include <memory>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>
#include <Common/setThreadName.h>

#include <Core/Mongo/Handler.h>
#include <Core/Mongo/MongoProtocol.h>

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsString default_session_user;
    extern const ServerSettingsUInt64 handshake_timeout_milliseconds;
}

namespace Setting
{
    extern const SettingsSeconds receive_timeout;
    extern const SettingsSeconds send_timeout;
    extern const SettingsUInt64 idle_connection_timeout;
}

MongoHandler::MongoHandler(
    const Poco::Net::StreamSocket & socket_,
    IServer & server_,
    TCPServer & tcp_server_,
    bool ssl_enabled_,
    Int32 connection_id_,
    std::optional<String> default_session_user_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , ssl_enabled(ssl_enabled_)
    , connection_id(connection_id_)
    , default_session_user(std::move(default_session_user_))
    , read_event(read_event_)
    , write_event(write_event_)
{
    changeIO(socket());
}

void MongoHandler::changeIO(Poco::Net::StreamSocket & socket)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket, read_event);
    out = std::make_shared<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket, write_event);
    message_transport = std::make_shared<MongoProtocol::MessageTransport>(in.get(), out.get());
}

void MongoHandler::run()
{
    setThreadName(ThreadName::MONGO_HANDLER);

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::MONGO);
    SCOPE_EXIT({ session.reset(); });

    session->setClientConnectionId(connection_id);

    /// An empty user name in `saslStart` means the default session user: the `default_session_user`
    /// server setting, possibly overridden for this listener in the `protocols` section.
    const String resolved_default_session_user = default_session_user
        ? *default_session_user
        : String(server.context()->getServerSettings()[ServerSetting::default_session_user]);

    /** The listener leaves the socket without a receive timeout, so the reads are bounded here: a
      * peer that stops sending in the middle of a message must not hold this thread and the buffer
      * of the message it announced forever.
      *
      * - Every read and write waits for at most `receive_timeout` and `send_timeout`.
      * - A whole message has to arrive within `handshake_timeout_milliseconds` until the connection
      *   has authenticated, the same budget the other protocols give their handshake, and within
      *   `receive_timeout` afterwards. A deadline per message rather than one for the whole login:
      *   a driver keeps monitoring connections that never authenticate, and only send `hello`.
      * - A connection that sends nothing is closed after `idle_connection_timeout`.
      */
    const auto & settings = server.context()->getSettingsRef();
    const Poco::Timespan receive_timeout = settings[Setting::receive_timeout];
    const UInt64 idle_connection_timeout = settings[Setting::idle_connection_timeout];
    const UInt64 handshake_timeout_ms = server.context()->getServerSettings()[ServerSetting::handshake_timeout_milliseconds];
    socket().setReceiveTimeout(receive_timeout);
    socket().setSendTimeout(settings[Setting::send_timeout]);

    try
    {
        while (tcp_server.isOpen())
        {
            constexpr size_t connection_check_timeout = 1; // 1 second
            Stopwatch idle_watch;
            while (!in->poll(1000000 * connection_check_timeout))
            {
                if (!tcp_server.isOpen())
                    return;
                if (idle_connection_timeout && idle_watch.elapsedSeconds() > static_cast<double>(idle_connection_timeout))
                {
                    LOG_TRACE(log, "Closing an idle Mongo connection");
                    return;
                }
            }

            const bool authenticated = !session->getClientInfo().current_user.empty();
            in->setHandshakeTimeout(authenticated ? static_cast<UInt64>(receive_timeout.totalMilliseconds()) : handshake_timeout_ms);

            /// A client that closed the connection is not an error.
            if (in->eof())
                return;

            /// A Mongo message is length-delimited by its header. Read the header, then read
            /// exactly the rest of the message and parse it from its own bounded buffer, so
            /// that the parser can neither run into the next message nor stop in the middle
            /// of this one when TCP splits or coalesces reads.
            auto header = message_transport->receive<MongoProtocol::Header>();
            String payload = message_transport->receivePayload(*header);
            in->clearHandshakeTimeout();
            ReadBufferFromString payload_buffer(payload);

            auto executor = std::make_shared<MongoProtocol::QueryExecutor>(session, socket().peerAddress(), resolved_default_session_user);
            MongoProtocol::handle(*header, payload_buffer, message_transport, executor);

            /// A login that failed has been answered, and nothing else may run in its session.
            if (executor->shouldCloseConnection())
                return;
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error while handling a Mongo connection");
    }
}

}

#endif
