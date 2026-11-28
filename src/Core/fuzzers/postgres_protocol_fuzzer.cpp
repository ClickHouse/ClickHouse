#include <Core/PostgreSQLProtocol.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ParserPreparedStatement.h>


/// The input is what `PostgreSQLHandler` reads from a client: the startup message (after an optional
/// SSL or GSSENC request), then typed messages. Parse, Bind and Execute go through the prepared statements
/// manager, which substitutes the bound parameters into the statement.

namespace DB::ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
}

using namespace DB;
using namespace DB::PostgreSQLProtocol::Messaging;
using DB::PostgreSQLProtocol::PostgresPreparedStatements::PreparedStatemetsManager;

namespace
{

void readFirstMessage(MessageTransport & transport, ReadBuffer & in)
{
    Int32 payload_size = 0;
    Int32 info = 0;
    auto read_header = [&]
    {
        readBinaryBigEndian(payload_size, in);
        if (payload_size < 8)
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Wrong initial message length");
        readBinaryBigEndian(info, in);
    };

    read_header();
    const auto type = static_cast<FrontMessageType>(info);
    if (type == FrontMessageType::SSL_REQUEST || type == FrontMessageType::GSSENC_REQUEST)
        read_header();

    if (static_cast<FrontMessageType>(info) == FrontMessageType::CANCEL_REQUEST)
    {
        std::ignore = transport.receiveWithPayloadSize<CancelRequest>(8);
        return;
    }

    if (payload_size < 9 || payload_size > max_unauthenticated_message_size)
        throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Wrong startup message length");
    std::ignore = transport.receiveWithPayloadSize<StartupMessage>(payload_size - 8);
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    if (size < 1)
        return 0;

    /// Both messages of the SCRAM exchange have the `PasswordMessage` type byte.
    const bool scram = data[0] & 1;

    ReadBufferFromMemory in(data + 1, size - 1);
    WriteBufferFromOwnString out;
    MessageTransport transport(&in, &out);
    PreparedStatemetsManager prepared_statements(/*limit_statements_=*/100);

    try
    {
        readFirstMessage(transport, in);

        size_t password_messages = 0;
        while (!in.eof())
        {
            try
            {
                switch (transport.receiveMessageType())
                {
                    case FrontMessageType::PASSWORD_MESSAGE:
                        if (!scram)
                            std::ignore = transport.receive<PasswordMessage>();
                        else if (password_messages == 0)
                            std::ignore = transport.receive<SASLInitialResponse>();
                        else
                            std::ignore = transport.receive<SASLResponse>();
                        ++password_messages;
                        break;
                    case FrontMessageType::QUERY:
                        std::ignore = transport.receive<Query>();
                        prepared_statements.dropUnnamedStatementAndPortal();
                        break;
                    case FrontMessageType::PARSE:
                    {
                        auto query = transport.receive<ParseQuery>();
                        auto statement = make_intrusive<ASTPreparedStatement>();
                        statement->function_name = query->function_name;
                        statement->function_body = query->sql_query;
                        statement->parameter_types = query->parameter_types;
                        prepared_statements.addStatement(statement.get());
                        break;
                    }
                    case FrontMessageType::BIND:
                        prepared_statements.attachBindQuery(transport.receive<BindQuery>());
                        break;
                    case FrontMessageType::EXECUTE:
                        std::ignore = transport.receive<ExecuteQuery>();
                        std::ignore = prepared_statements.getStatmentFromBind();
                        break;
                    case FrontMessageType::DESCRIBE:
                        std::ignore = transport.receive<DescribeQuery>();
                        break;
                    case FrontMessageType::CLOSE:
                    {
                        auto close = transport.receive<CloseQuery>();
                        prepared_statements.tryDeleteStatement(close->function_name);
                        break;
                    }
                    case FrontMessageType::SYNC:
                        std::ignore = transport.receive<SyncQuery>();
                        break;
                    case FrontMessageType::COPY_COMPLETION:
                        std::ignore = transport.receive<CopyDone>();
                        break;
                    case FrontMessageType::COPY_FAILURE:
                        std::ignore = transport.receive<CopyFail>();
                        break;
                    case FrontMessageType::TERMINATE:
                        std::ignore = transport.receive<Terminate>();
                        return 0;
                    default:
                        transport.dropMessage();
                        break;
                }
            }
            catch (const Exception & e)
            {
                /// The handler answers an error in a message and goes on with the next one,
                /// unless the stream cannot be read any further.
                if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF || e.code() == ErrorCodes::CANNOT_READ_ALL_DATA)
                    throw;
            }
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed messages are rejected with exceptions.
    }

    return 0;
}
