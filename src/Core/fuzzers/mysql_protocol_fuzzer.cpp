#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <IO/LimitReadBuffer.h>
#include <IO/MySQLPacketPayloadReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>


/// The input is what `MySQLHandler` reads from a client: an optional SSL request, the handshake response,
/// an optional auth switch response, then commands. Each packet has its 4-byte header with the sequence id.

using namespace DB;
using namespace DB::MySQLProtocol;

namespace
{

/// A handshake response must be read back the same after it is written.
void checkRoundTrip(const ConnectionPhase::HandshakeResponse & response)
{
    WriteBufferFromOwnString out;
    uint8_t sequence_id = 0;
    response.writePayload(out, sequence_id);
    out.finalize();

    ReadBufferFromString in(out.str());
    sequence_id = 0;
    ConnectionPhase::HandshakeResponse reread;
    reread.readPayload(in, sequence_id);
    if (reread.capability_flags != response.capability_flags || reread.max_packet_size != response.max_packet_size
        || reread.character_set != response.character_set || reread.username != response.username
        || reread.database != response.database || reread.auth_response != response.auth_response
        || reread.auth_plugin_name != response.auth_plugin_name || !in.eof())
        __builtin_trap();
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    if (size < 1)
        return 0;

    const bool ssl = data[0] & 1;
    const bool auth_switch = data[0] & 2;

    ReadBufferFromMemory in(data + 1, size - 1);
    try
    {
        uint8_t sequence_id = 1;
        if (ssl)
        {
            Generic::SSLRequest ssl_request;
            ssl_request.readPayload(in, sequence_id);
        }

        ConnectionPhase::HandshakeResponse handshake_response;
        handshake_response.readPayload(in, sequence_id);
        checkRoundTrip(handshake_response);

        if (auth_switch)
        {
            ConnectionPhase::AuthSwitchResponse auth_switch_response;
            auth_switch_response.readPayload(in, sequence_id);
        }

        while (!in.eof())
        {
            sequence_id = 0;
            MySQLPacketPayloadReadBuffer payload(in, sequence_id);
            char command = 0;
            payload.readStrict(command);

            LimitReadBuffer limited_payload(payload, {.read_no_more = 1000, .expect_eof = true});
            switch (command)
            {
                case Generic::COM_QUIT:
                    return 0;
                case Generic::COM_INIT_DB:
                {
                    String database;
                    readStringUntilEOF(database, limited_payload);
                    break;
                }
                case Generic::COM_FIELD_LIST:
                {
                    ProtocolText::ComFieldList packet;
                    packet.readPayloadWithUnpacked(limited_payload);
                    break;
                }
                case Generic::COM_STMT_EXECUTE:
                case Generic::COM_STMT_CLOSE:
                case Generic::COM_STMT_RESET:
                {
                    uint32_t statement_id = 0;
                    payload.readStrict(reinterpret_cast<char *>(&statement_id), sizeof(statement_id));
                    break;
                }
                default:
                    break;
            }
            payload.ignoreAll();
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        /// Malformed packets are rejected with exceptions.
    }

    return 0;
}
