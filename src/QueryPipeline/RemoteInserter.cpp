#include <QueryPipeline/RemoteInserter.h>

#include <Client/Connection.h>
#include <Client/SecondaryQuerySettings.h>
#include <Common/logger_useful.h>

#include <Common/NetException.h>
#include <Common/CurrentThread.h>
#include <Interpreters/ClientInfo.h>
#include <Interpreters/InternalTextLogsQueue.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProfileEventsExt.h>
#include <IO/ConnectionTimeouts.h>
#include <Core/Settings.h>


namespace ProfileEvents
{
    extern const Event InsertedRows;
    extern const Event InsertedBytes;
}

namespace DB
{

namespace Setting
{
    extern const SettingsLogsLevel send_logs_level;
    extern const SettingsBool send_profile_events;
}

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
}


RemoteInserter::RemoteInserter(
    Connection & connection_,
    const ConnectionTimeouts & timeouts_,
    const String & query_,
    const Settings & settings_,
    const ClientInfo & client_info_,
    QueryStatusPtr process_list_element_)
    : insert_settings(settings_)
    , client_info(client_info_)
    , timeouts(timeouts_)
    , process_list_element(std::move(process_list_element_))
    , connection(connection_)
    , query(query_)
    , server_revision(connection.getServerRevision(timeouts))
{}

void RemoteInserter::initialize()
{
    ClientInfo modified_client_info = client_info;
    modified_client_info.query_kind = ClientInfo::QueryKind::SECONDARY_QUERY;
    /// Drop inherited current_roles: a write needs no role scoping.
    modified_client_info.current_roles.reset();

    Settings settings = insert_settings;

    /// Demote the `compatibility`-derived values and force ClickHouse SQL, exactly as the `SELECT`
    /// senders do. Runs before the overrides below, so they stay changed and are serialized.
    prepareSecondaryQuerySettings(settings);

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

    /// The quotas over profile events charge the initiating user with what the remote server reports in
    /// its `ProfileEvents` packets, so the reports must not depend on `send_profile_events` of the query
    /// or of the user's profile on the remote server, the same as for `SELECT`. There is no deadlock like
    /// with `send_logs_level` above: the remote server sends the profile events of a secondary `INSERT`
    /// only once, after it has received all the data, when this side is already waiting in `onFinish`.
    settings[Setting::send_profile_events] = true;
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
        else if (Protocol::Server::ProfileEvents == packet.type)
        {
            /// A subquery executed while the remote server is still analysing the query reports profile
            /// events, so these can arrive before the header block.
            processProfileEventsPacket(packet.block);
        }
        else if (Protocol::Server::Progress == packet.type)
        {
            /// The same as above, the progress of such a subquery is ignored.
        }
        else
            throw NetException(
                ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                "Unexpected packet from server (expected Data, Exception, Log, TableColumns, Progress "
                "or ProfileEvents, got {})",
                Protocol::Server::toString(packet.type));
    }
}


void RemoteInserter::write(Block block)
{
    sent_rows += block.rows();
    sent_bytes += block.bytes();

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
        else if (Protocol::Server::ProfileEvents == packet.type)
            processProfileEventsPacket(packet.block);
        else if (Protocol::Server::Log == packet.type ||
            Protocol::Server::Progress == packet.type ||
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

void RemoteInserter::processProfileEventsPacket(Block block)
{
    /// The rows and bytes sent to the remote server are counted locally (see `CountingTransform`) before they
    /// are sent, and the remote server counts them again when it writes them. Only what it writes beyond them,
    /// e.g. into the target tables of materialized views, is new. The reports of the servers further down
    /// are forwarded verbatim in the same packets, but they are deduplicated by the same rule at their own hop,
    /// so subtracting what this side has sent from the sum of the packet keeps the total exact.
    ProfileEvents::subtractIncrements(block, ProfileEvents::InsertedRows, sent_rows);
    ProfileEvents::subtractIncrements(block, ProfileEvents::InsertedBytes, sent_bytes);

    if (process_list_element)
        process_list_element->addRemoteProfileEvents(block, /* from_insert = */ true);

    /// Pass the profile events of the remote server to the client, as `RemoteQueryExecutor` does. When this
    /// server executes a secondary `INSERT` itself (one more distributed hop), it is how the reports of the
    /// servers further down reach the initiator and its quotas over profile events.
    if (auto profile_queue = CurrentThread::getInternalProfileEventsQueue())
        if (!profile_queue->emplace(std::move(block)))
            throw Exception(ErrorCodes::SYSTEM_ERROR, "Could not push into profile queue");
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
