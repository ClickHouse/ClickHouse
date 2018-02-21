#include <DataStreams/RemoteBlockOutputStream.h>

#include <Client/Connection.h>
#include <common/logger_useful.h>

#include <Common/NetException.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int LOGICAL_ERROR;
}


RemoteBlockOutputStream::RemoteBlockOutputStream(Connection & connection_, const String & query_, const Settings * settings_)
    : connection(connection_), query(query_), settings(settings_)
{
    /** Send query and receive "header", that describe table structure.
      * Header is needed to know, what structure is required for blocks to be passed to 'write' method.
      */
    connection.sendQuery(query, "", QueryProcessingStage::Complete, settings, nullptr);

    Connection::Packet packet = connection.receivePacket();

    if (Protocol::Server::Data == packet.type)
    {
        header = packet.block;

        if (!header)
            throw Exception("Logical error: empty block received as table structure", ErrorCodes::LOGICAL_ERROR);
    }
    else if (Protocol::Server::Exception == packet.type)
    {
        packet.exception->rethrow();
        return;
    }
    else
        throw NetException("Unexpected packet from server (expected Data or Exception, got "
            + String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
}


void RemoteBlockOutputStream::write(const Block & block)
{
    assertBlocksHaveEqualStructure(block, header, "RemoteBlockOutputStream");
    connection.sendData(block);
}


void RemoteBlockOutputStream::writePrepared(ReadBuffer & input, size_t size)
{
    /// We cannot use 'header'. Input must contain block with proper structure.
    connection.sendPreparedData(input, size);
}


void RemoteBlockOutputStream::writeSuffix()
{
    /// Empty block means end of data.
    connection.sendData(Block());

    /// Receive EndOfStream packet.
    Connection::Packet packet = connection.receivePacket();

    if (Protocol::Server::EndOfStream == packet.type)
    {
        /// Do nothing.
    }
    else if (Protocol::Server::Exception == packet.type)
        packet.exception->rethrow();
    else
        throw NetException("Unexpected packet from server (expected EndOfStream or Exception, got "
        + String(Protocol::Server::toString(packet.type)) + ")", ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);

    finished = true;
}

RemoteBlockOutputStream::~RemoteBlockOutputStream()
{
    /// If interrupted in the middle of the loop of communication with the server, then interrupt the connection,
    ///  to not leave the connection in unsynchronized state.
    if (!finished)
    {
        try
        {
            connection.disconnect();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

}
