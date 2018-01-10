#pragma once

#include <common/logger_useful.h>

#include <Poco/Net/StreamSocket.h>

#include <Common/Throttler.h>

#include <Core/Block.h>
#include <Core/Defines.h>
#include <IO/Progress.h>
#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/BlockStreamProfileInfo.h>

#include <IO/CompressionSettings.h>
#include <IO/ConnectionTimeouts.h>

#include <Interpreters/Settings.h>
#include <Interpreters/TablesStatus.h>

#include <atomic>


namespace DB
{

class ClientInfo;

/// The stream of blocks reading from the table and its name
using ExternalTableData = std::pair<BlockInputStreamPtr, std::string>;
/// Vector of pairs describing tables
using ExternalTablesData = std::vector<ExternalTableData>;

class Connection;

using ConnectionPtr = std::shared_ptr<Connection>;
using Connections = std::vector<ConnectionPtr>;


/** Connection with database server, to use by client.
  * How to use - see Core/Protocol.h
  * (Implementation of server end - see Server/TCPHandler.h)
  *
  * As 'default_database' empty string could be passed
  *  - in that case, server will use it's own default database.
  */
class Connection : private boost::noncopyable
{
    friend class MultiplexedConnections;

public:
    Connection(const String & host_, UInt16 port_, const String & default_database_,
        const String & user_, const String & password_,
        const ConnectionTimeouts & timeouts_,
        const String & client_name_ = "client",
        Protocol::Compression compression_ = Protocol::Compression::Enable,
        Protocol::Encryption encryption_ = Protocol::Encryption::Disable,
        Poco::Timespan sync_request_timeout_ = Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0))
        :
        host(host_), port(port_), default_database(default_database_),
        user(user_), password(password_), resolved_address(host, port),
        client_name(client_name_),
        compression(compression_),
        encryption(encryption_),
        timeouts(timeouts_),
        sync_request_timeout(sync_request_timeout_),
        log_wrapper(*this)
    {
        /// Don't connect immediately, only on first need.

        if (user.empty())
            user = "default";

        setDescription();
    }

    Connection(const String & host_, UInt16 port_, const Poco::Net::SocketAddress & resolved_address_,
        const String & default_database_,
        const String & user_, const String & password_,
        const ConnectionTimeouts & timeouts_,
        const String & client_name_ = "client",
        Protocol::Compression compression_ = Protocol::Compression::Enable,
        Protocol::Encryption encryption_ = Protocol::Encryption::Disable,
        Poco::Timespan sync_request_timeout_ = Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0))
        :
        host(host_), port(port_),
        default_database(default_database_),
        user(user_), password(password_),
        resolved_address(resolved_address_),
        client_name(client_name_),
        compression(compression_),
        encryption(encryption_),
        timeouts(timeouts_),
        sync_request_timeout(sync_request_timeout_),
        log_wrapper(*this)
    {
        /// Don't connect immediately, only on first need.

        if (user.empty())
            user = "default";

        setDescription();
    }

    virtual ~Connection() {};

    /// Set throttler of network traffic. One throttler could be used for multiple connections to limit total traffic.
    void setThrottler(const ThrottlerPtr & throttler_)
    {
        throttler = throttler_;
    }


    /// Packet that could be received from server.
    struct Packet
    {
        UInt64 type;

        Block block;
        std::unique_ptr<Exception> exception;
        Progress progress;
        BlockStreamProfileInfo profile_info;

        Packet() : type(Protocol::Server::Hello) {}
    };

    /// Change default database. Changes will take effect on next reconnect.
    void setDefaultDatabase(const String & database);

    void getServerVersion(String & name, UInt64 & version_major, UInt64 & version_minor, UInt64 & revision);

    const String & getServerTimezone();

    /// For log and exception messages.
    const String & getDescription() const;
    const String & getHost() const;
    UInt16 getPort() const;
    const String & getDefaultDatabase() const;

    /// If last flag is true, you need to call sendExternalTablesData after.
    void sendQuery(
        const String & query,
        const String & query_id_ = "",
        UInt64 stage = QueryProcessingStage::Complete,
        const Settings * settings = nullptr,
        const ClientInfo * client_info = nullptr,
        bool with_pending_data = false);

    void sendCancel();
    /// Send block of data; if name is specified, server will write it to external (temporary) table of that name.
    void sendData(const Block & block, const String & name = "");
    /// Send all contents of external (temporary) tables.
    void sendExternalTablesData(ExternalTablesData & data);

    /// Send prepared block of data (serialized and, if need, compressed), that will be read from 'input'.
    /// You could pass size of serialized/compressed block.
    void sendPreparedData(ReadBuffer & input, size_t size, const String & name = "");

    /// Check, if has data to read.
    bool poll(size_t timeout_microseconds = 0);

    /// Check, if has data in read buffer.
    bool hasReadBufferPendingData() const;

    /// Receive packet from server.
    Packet receivePacket();

    /// If not connected yet, or if connection is broken - then connect. If cannot connect - throw an exception.
    void forceConnected();

    TablesStatusResponse getTablesStatus(const TablesStatusRequest & request);

    /** Disconnect.
      * This may be used, if connection is left in unsynchronised state
      *  (when someone continues to wait for something) after an exception.
      */
    void disconnect();

    /** Fill in the information that is needed when getting the block for some tasks
      * (so far only for a DESCRIBE TABLE query with Distributed tables).
      */
    void fillBlockExtraInfo(BlockExtraInfo & info) const;

    size_t outBytesCount() const { return out ? out->count() : 0; }
    size_t inBytesCount() const { return in ? in->count() : 0; }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;

    /** Address could be resolved beforehand and passed to constructor. Then 'host' and 'port' fields are used just for logging.
      * Otherwise address is resolved in constructor. Thus, DNS based load balancing is not supported.
      */
    Poco::Net::SocketAddress resolved_address;

    /// For messages in log and in exceptions.
    String description;
    void setDescription();

    String client_name;

    bool connected = false;

    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_revision = 0;
    String server_timezone;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    String query_id;
    Protocol::Compression compression;        /// Enable data compression for communication.
    Protocol::Encryption encryption;             /// Enable data encryption for communication.

    /// What compression settings to use while sending data for INSERT queries and external tables.
    CompressionSettings compression_settings;

    /** If not nullptr, used to limit network traffic.
      * Only traffic for transferring blocks is accounted. Other packets don't.
      */
    ThrottlerPtr throttler;

    ConnectionTimeouts timeouts;
    Poco::Timespan sync_request_timeout;

    /// From where to read query execution result.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    BlockInputStreamPtr block_in;

    /// Where to write data for INSERT.
    std::shared_ptr<WriteBuffer> maybe_compressed_out;
    BlockOutputStreamPtr block_out;

    /// Logger is created lazily, for avoid to run DNS request in constructor.
    class LoggerWrapper
    {
    public:
        LoggerWrapper(Connection & parent_)
            : log(nullptr), parent(parent_)
        {
        }

        Logger * get()
        {
            if (!log)
                log = &Logger::get("Connection (" + parent.getDescription() + ")");

            return log;
        }

    private:
        std::atomic<Logger *> log;
        Connection & parent;
    };

    LoggerWrapper log_wrapper;

    void connect();
    void sendHello();
    void receiveHello();
    bool ping();

    Block receiveData();
    std::unique_ptr<Exception> receiveException();
    Progress receiveProgress();
    BlockStreamProfileInfo receiveProfileInfo();

    void initBlockInput();

    void throwUnexpectedPacket(UInt64 packet_type, const char * expected) const;
};

}
