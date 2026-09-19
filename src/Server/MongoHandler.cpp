#include <Server/MongoHandler.h>

/// The Mongo wire protocol needs BSON (mongo-cxx-driver) and the Mongo dialect (rapidjson).
#if USE_MONGODB && USE_RAPIDJSON

#include <memory>
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
#include <Common/setThreadName.h>

#include <Core/Mongo/Handler.h>
#include <Core/Mongo/MongoProtocol.h>

namespace DB
{

MongoHandler::MongoHandler(
    const Poco::Net::StreamSocket & socket_,
    IServer & server_,
    TCPServer & tcp_server_,
    bool ssl_enabled_,
    Int32 connection_id_,
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , ssl_enabled(ssl_enabled_)
    , connection_id(connection_id_)
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

    try
    {
        while (tcp_server.isOpen())
        {
            constexpr size_t connection_check_timeout = 1; // 1 second
            while (!in->poll(1000000 * connection_check_timeout))
                if (!tcp_server.isOpen())
                    return;

            /// A client that closed the connection is not an error.
            if (in->eof())
                return;

            /// A Mongo message is length-delimited by its header. Read the header, then read
            /// exactly the rest of the message and parse it from its own bounded buffer, so
            /// that the parser can neither run into the next message nor stop in the middle
            /// of this one when TCP splits or coalesces reads.
            auto header = message_transport->receive<MongoProtocol::Header>();
            String payload = message_transport->receivePayload(*header);
            ReadBufferFromString payload_buffer(payload);

            auto executor = std::make_shared<MongoProtocol::QueryExecutor>(session, socket().peerAddress());
            MongoProtocol::handle(*header, payload_buffer, message_transport, executor);
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
