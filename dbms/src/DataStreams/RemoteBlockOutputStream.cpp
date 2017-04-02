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
}


void RemoteBlockOutputStream::writePrefix()
{
    /** Send query and receive "sample block", that describe table structure.
      * Sample block is needed to know, what structure is required for blocks to be passed to 'write' method.
      */

    connection.sendQuery(query, "", QueryProcessingStage::Complete, settings, nullptr);

    Connection::Packet packet = connection.receivePacket();

    if (Protocol::Server::Data == packet.type)
    {
        sample_block = packet.block;

        if (!sample_block)
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
    if (!sample_block)
        throw Exception("You must call IBlockOutputStream::writePrefix before IBlockOutputStream::write", ErrorCodes::LOGICAL_ERROR);

    if (!blocksHaveEqualStructure(block, sample_block))
    {
        std::stringstream message;
        message << "Block structure is different from table structure.\n"
        << "\nTable structure:\n(" << sample_block.dumpStructure() << ")\nBlock structure:\n(" << block.dumpStructure() << ")\n";

        LOG_ERROR(&Logger::get("RemoteBlockOutputStream"), message.str());
        throw DB::Exception(message.str());
    }

    connection.sendData(block);
}


void RemoteBlockOutputStream::writePrepared(ReadBuffer & input, size_t size)
{
    /// We cannot use 'sample_block'. Input must contain block with proper structure.
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
}

}
