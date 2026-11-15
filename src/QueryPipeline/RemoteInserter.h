#pragma once

#include <Core/Block.h>
#include <IO/ConnectionTimeouts.h>

#include <memory>

namespace DB
{

class ClientInfo;
class Connection;
class ReadBuffer;
struct Settings;
class QueryStatus;
using QueryStatusPtr = std::shared_ptr<QueryStatus>;


/** Allow to execute INSERT query on remote server and send data for it.
  */
class RemoteInserter
{
public:
    RemoteInserter(
        Connection & connection_,
        const ConnectionTimeouts & timeouts,
        const String & query_,
        const Settings & settings_,
        const ClientInfo & client_info_,
        QueryStatusPtr process_list_element_ = nullptr);

    void initialize();

    Block initializeAndGetHeader()
    {
        initialize();
        return header;
    }

    void write(Block block);
    void onFinish();

    /// Send pre-serialized and possibly pre-compressed block of data, that will be read from 'input'.
    void writePrepared(ReadBuffer & buf, size_t size = 0);

    ~RemoteInserter();

    const Block & getHeader() const { return header; }
    UInt64 getServerRevision() const { return server_revision; }

private:
    /// Accounts the `ProfileEvents` packet of the remote server for the quotas over profile events of
    /// the query this insert is a part of, if any.
    void processProfileEventsPacket(Block block);

    const Settings & insert_settings;
    const ClientInfo & client_info;
    const ConnectionTimeouts & timeouts;

    /// The query on behalf of which the data is inserted, to charge it with the profile events of the
    /// remote server. Not set for the background sends of `Distributed`, which run on behalf of no query.
    QueryStatusPtr process_list_element;

    Connection & connection;
    String query;

    Block header;
    bool finished = false;

    /// What has been sent by `write` and not yet subtracted from the `InsertedRows` / `InsertedBytes`
    /// reported by the remote server; see `processProfileEventsPacket`.
    UInt64 sent_rows = 0;
    UInt64 sent_bytes = 0;
    UInt64 server_revision;
};

}
