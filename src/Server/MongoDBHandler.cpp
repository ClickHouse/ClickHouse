#include "MongoDBHandler.h"
#include <Core/MongoDB/Commands/Commands.h>
#include <Core/MongoDB/Commands/Handlers.h>
#include <Core/MongoDB/Messages/Message.h>
#include <Core/MongoDB/Messages/MessageHeader.h>
#include <Core/MongoDB/Messages/MessageReader.h>
#include <Core/MongoDB/Messages/MessageWriter.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <base/types.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <pcg_random.hpp>
#include <Poco/Exception.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>

#include <memory>

namespace DB
{


MongoDBHandler::MongoDBHandler(const Poco::Net::StreamSocket & socket_, IServer & server_, TCPServer & tcp_server_, Int32 connection_id_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_), connection_id(connection_id_)
{
}


void MongoDBHandler::handleQuery(MongoDB::OpMsgMessage::Ptr message, DB::MongoDB::MessageWriter writer)
{
    auto command = MongoDB::Command::parseCommand(message);
    BSON::Document::Ptr res;

    pcg64_fast gen{randomSeed()};
    std::uniform_int_distribution<Int32> dis(0, INT32_MAX);
    auto secret_key = dis(gen);

    ContextMutablePtr context;
    if (command->getDBName() != "admin")
    {
        // TODO support admin commands like getParameter, Hello and other
        // NOTE: currently supported operations with regular dbs&tables
        session->sessionContext()->setCurrentDatabase(command->getDBName());
        context = session->makeQueryContext();
        context->setCurrentQueryId(fmt::format("mongodb:{:d}:{:d}", connection_id, secret_key));
        LOG_DEBUG(log, "Made context for queries");
    }
    try
    {
        switch (command->getType())
        {
            case MongoDB::CommandTypes::IsMaster:
                res = MongoDB::handleIsMaster();
                writer.write_op_reply(res, message->getHeader().getRequestID());
                return;
            case MongoDB::CommandTypes::Hello:
                res = MongoDB::handleIsMaster();
                break;
            case MongoDB::CommandTypes::BuildInfo:
                res = MongoDB::handleBuildInfo();
                break;
            case MongoDB::CommandTypes::GetParameter:
                res = MongoDB::handleGetParameter(command);
                break;
            case MongoDB::CommandTypes::Ping:
                res = MongoDB::handlePing();
                break;
            case MongoDB::CommandTypes::GetLog:
                res = MongoDB::handleGetLog(command);
                break;
            case MongoDB::CommandTypes::Aggregate:
                if (command->getDBName() == "admin")
                    res = MongoDB::handleAtlasCLI(command);
                else
                    res = MongoDB::handleAggregate(command, context);
                break;
            case MongoDB::CommandTypes::Find: {
                res = MongoDB::handleFind(command, context);
            }
            break;
            case MongoDB::CommandTypes::Unknown:
                LOG_ERROR(log, "Unknown MongoDB command");
                res = MongoDB::handleUnknownCommand(command);
                break;
        }
    }
    catch (const Poco::Exception & e)
    {
        res = MongoDB::handleError(e.displayText());
    }
    catch (std::exception & e)
    {
        res = MongoDB::handleError(e.what());
    }
    writer.write(res, message->getHeader().getRequestID());
}

void MongoDBHandler::run()
{
    LOG_INFO(log, "MongoDB has new connection");
    setThreadName(this->handler_name);
    ThreadStatus thread_status;

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::MONGODB);

    session->authenticate("default", "", socket().peerAddress()); // TODO make authentication
    session->makeSessionContext();
    SCOPE_EXIT({ session.reset(); });

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    session->setClientConnectionId(connection_id);
    DB::MongoDB::MessageReader reader(*in);
    DB::MongoDB::MessageWriter writer(*out);

    while (tcp_server.isOpen())
    {
        while (!in->poll(1'000'000))
            if (!tcp_server.isOpen())
                return;
        MongoDB::Message::Ptr message;
        try
        {
            message = reader.read();
        }
        catch (const DB::Exception & e)
        {
            if (!tcp_server.isOpen())
                LOG_INFO(log, "Closing connection");
            else
                LOG_WARNING(log, "Read error {}, but tcp is open, closing", e.what());
            return;
        }
        MongoDB::MessageHeader::OpCode op_code = message->getHeader().getOpCode();
        LOG_INFO(log, "GOT OPCODE {}, request_id: {}", static_cast<int>(op_code), message->getHeader().getRequestID());
        switch (op_code)
        {
            case MongoDB::MessageHeader::OP_QUERY: {
                LOG_INFO(log, "Handling Hello message");
                BSON::Document::Ptr response = MongoDB::handleIsMaster();
                writer.write_op_reply(response, message->getHeader().getRequestID());
                LOG_INFO(log, "Sent Hello message");
            }
            break;
            case MongoDB::MessageHeader::OP_MSG: {
                handleQuery(message.cast<MongoDB::OpMsgMessage>(), writer);
                LOG_INFO(log, "QUERY HANDLED");
            }
            break;
            default:
                throw Poco::NotImplementedException("MongoDB Handler: Unknown OpCode");
        }
        in->next();
    }
    out->finalize();
}

}
