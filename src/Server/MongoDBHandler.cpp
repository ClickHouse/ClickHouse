#include "MongoDBHandler.h"
#include <Core/MongoDB/Commands/MessageHandler.h>
#include <Core/MongoDB/Message.h>
#include <Core/MongoDB/MessageHeader.h>
#include <Core/MongoDB/MessageReader.h>
#include <Core/MongoDB/MessageWriter.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/parseQuery.h>
#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <base/types.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <pcg_random.hpp>
#include <Poco/Exception.h>
#include <Common/BSONParser/Array.h>
#include <Common/config_version.h>
#include <Common/randomSeed.h>
#include <Common/setThreadName.h>

#include <memory>

#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{

namespace BSON
{

Int32 Document::read(ReadBuffer & reader)
{
    Int32 size;
    try
    {
        readIntBinary(size, reader);
    }
    catch (const std::exception &)
    {
        return 0;
    }

    char type;
    if (!reader.read(type))
        throw std::exception(); // FIXME


    while (type != '\0')
    {
        Element::Ptr element;

        std::string name;
        readNullTerminated(name, reader);

        switch (type)
        {
            case ElementTraits<double>::TypeId:
                element = new ConcreteElement<double>(name, 0);
                break;
            case ElementTraits<Int32>::TypeId:
                element = new ConcreteElement<Int32>(name, 0);
                break;
            case ElementTraits<std::string>::TypeId:
                element = new ConcreteElement<std::string>(name, "");
                break;
            case ElementTraits<Document::Ptr>::TypeId:
                element = new ConcreteElement<Document::Ptr>(name, new Document);
                break;
            case ElementTraits<bool>::TypeId:
                element = new ConcreteElement<bool>(name, false);
                break;
            case ElementTraits<Int64>::TypeId:
                element = new ConcreteElement<Int64>(name, 0);
                break;
            case ElementTraits<Array::Ptr>::TypeId:
                element = new ConcreteElement<Array::Ptr>(name, new Array);
                break;
            default: {
                std::stringstream ss;
                ss << "Element " << name << " contains an unsupported type 0x" << std::hex << static_cast<int>(type);
                throw Poco::NotImplementedException(ss.str());
            }
                // TODO: everything else:)
        }
        element->read(reader);
        elements.push_back(element);

        if (!reader.read(type))
            throw std::exception(); // FIXME
    }
    return size;
}

Array & Document::addNewArray(const std::string & name)
{
    Array::Ptr new_array = new Array();
    add(name, new_array);
    return *new_array;
}
}


MongoDBHandler::MongoDBHandler(const Poco::Net::StreamSocket & socket_, IServer & server_, TCPServer & tcp_server_, Int32 connection_id_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_), connection_id(connection_id_)
{
}

void MongoDBHandler::run()
{
    LOG_INFO(log, "MongoDB has new connection");
    setThreadName(this->handler_name);
    ThreadStatus thread_status;

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::MONGODB);
    SCOPE_EXIT({ session.reset(); });

    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    session->setClientConnectionId(connection_id);
    DB::MongoDB::MessageReader reader(*in);
    DB::MongoDB::MessageWriter writer(*out);
    { // HELLO
        MongoDB::Message::Ptr message = reader.read();
        MongoDB::MessageHeader::OpCode op_code = message->getHeader().getOpCode();
        LOG_INFO(log, "GOT OPCODE {}", static_cast<int>(op_code));
        switch (op_code)
        {
            case MongoDB::MessageHeader::OP_QUERY: {
                BSON::Document::Ptr response = MongoDB::MessageHandler::handleIsMaster();
                writer.writeHelloCmd(response, message->getHeader().getRequestID());
            }
            break;
            default:
                throw Poco::NotImplementedException();
        }
    }
    while (tcp_server.isOpen())
    {
        while (!in->poll(1'000'000))
            if (!tcp_server.isOpen())
                return;
    }
    out->finalize();
}

} // namespace DB
