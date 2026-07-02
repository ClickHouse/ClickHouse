#include <QueryPipeline/RemoteInserter.h>

#include <Client/Connection.h>
#include <Common/logger_useful.h>

#include <Common/NetException.h>
#include <Common/CurrentThread.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>


namespace DB
{

namespace Setting
{
    extern const SettingsLogsLevel send_logs_level;
    extern const SettingsString query_rules;
}

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}


RemoteInserter::RemoteInserter(
    Connection & connection_,
    const ConnectionTimeouts & timeouts_,
    const String & query_,
    const Settings & settings_,
    const ClientInfo & client_info_)
    : insert_settings(settings_)
    , client_info(client_info_)
    , timeouts(timeouts_)
    , connection(connection_)
    , query(query_)
    , server_revision(connection.getServerRevision(timeouts))
{}

void RemoteInserter::initialize()
{
    ClientInfo modified_client_info = client_info;
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;

    Settings settings = insert_settings;
    /// With current protocol it is impossible to avoid deadlock in case of send_logs_level!=none.
    ///
    /// RemoteInserter send Data blocks/packets to the remote shard,
    /// while remote side can send Log packets to the initiator (this RemoteInserter instance).
    ///
    /// But it is not enough to pull Log packets just before writing the next block
    /// since there is no way to ensure that all Log packets had been consumed.
    ///
    /// And if enough Log packets will be queued by the remote side,
    /// it will wait send_timeout until initiator will consume those packets,
    /// while initiator already starts writing Data blocks,
    /// and will not consume Log packets.
    ///
    /// So that is why send_logs_level had been disabled here.
    settings[Setting::send_logs_level] = "none";

    /// Rewrite rules are applied once, on the initiator, before the query is distributed.
    /// Strip `query_rules` from the settings sent to the shard for this secondary INSERT: the
    /// fragment was already produced from the rewritten query on the initiator, a rule named here
    /// may not even exist on the shard (rule storage is local by default), and re-applying it
    /// would rewrite/reject the fragment a second time. Send an explicit empty override (kept
    /// `changed`) so it also overrides any shard-side profile default for `query_rules`; an older
    /// shard that does not know the setting safely ignores it. This mirrors the read-path strip in
    /// `MultiplexedConnections::sendQuery` / `HedgedConnections::sendQuery`, and — like there — does
    /// not rely on the client-controlled `query_kind`, which can be spoofed.
    settings[Setting::query_rules] = "";

    /** Send query and receive "header", that describes table structure.
      * Header is needed to know, what structure is required for blocks to be passed to 'write' method.
      */
    /// TODO (vnemkov): figure out should we pass additional roles in this case or not.
    connection.sendQuery(
        timeouts, query, /* query_parameters */ {}, "", QueryProcessingStage::Complete, &settings, &modified_client_info, false, /* external_roles */ {}, {});

    while (true)
    {
        Packet packet = connection.receivePacket();

        if (Protocol::Server::Data == packet.type)
        {
            header = packet.block;
            break;
        }
        if (Protocol::Server::Exception == packet.type)
        {
            packet.exception->rethrow();
            break;
        }
        if (Protocol::Server::Log == packet.type)
        {
            /// Pass logs from remote server to client
            if (auto log_queue = CurrentThread::getInternalTextLogsQueue())
                log_queue->pushBlock(std::move(packet.block));
        }
        else if (Protocol::Server::TableColumns == packet.type)
        {
            /// Server could attach ColumnsDescription in front of stream for column defaults. There's no need to pass it through cause
            /// client's already got this information for remote table. Ignore.
        }
        else if (Protocol::Server::Progress == packet.type)
        {
            /// Progress packets are ignored
        }
        else
            throw NetException(
                ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                "Unexpected packet from server (expected Data or Exception, got {})",
                Protocol::Server::toString(packet.type));
    }
}


void RemoteInserter::write(Block block)
{
    try
    {
        connection.sendData(block, /* name */"", /* scalar */false);
    }
    catch (const NetException &)
    {
        /// Try to get more detailed exception from server
        auto packet_type = connection.checkPacket(/* timeout_microseconds */0);
        if (packet_type && *packet_type == Protocol::Server::Exception)
        {
            Packet packet = connection.receivePacket();
            packet.exception->rethrow();
        }

        throw;
    }
}


void RemoteInserter::writePrepared(ReadBuffer & buf, size_t size)
{
    /// We cannot use 'header'. Input must contain block with proper structure.
    connection.sendPreparedData(buf, size);
}


void RemoteInserter::onFinish()
{
    /// Empty block means end of data.
    connection.sendData(Block(), /* name */"", /* scalar */false);

    /// Wait for EndOfStream or Exception packet, skip Log packets.
    while (true)
    {
        Packet packet = connection.receivePacket();

        if (Protocol::Server::EndOfStream == packet.type)
            break;

        if (Protocol::Server::Exception == packet.type)
            packet.exception->rethrow();
        else if (Protocol::Server::Log == packet.type ||
            Protocol::Server::Progress == packet.type ||
            Protocol::Server::ProfileEvents == packet.type ||
            Protocol::Server::TimezoneUpdate == packet.type)
        {
            // Do nothing
        }
        else
            throw NetException(
                ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                "Unexpected packet from server (expected EndOfStream or Exception, got {})",
                Protocol::Server::toString(packet.type));
    }

    finished = true;
}

RemoteInserter::~RemoteInserter()
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
