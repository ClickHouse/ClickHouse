#pragma once

#include <common/logger_useful.h>

#include <Poco/Net/StreamSocket.h>

#include <Common/Throttler.h>

#include <Core/Block.h>
#include <Core/Defines.h>
#include <IO/Progress.h>
#include <Core/Protocol.h>
#include <Core/QueryProcessingStage.h>

#include <DataStreams/IBlockStream_fwd.h>
#include <DataStreams/BlockStreamProfileInfo.h>

#include <IO/ConnectionTimeouts.h>

#include <Core/Settings.h>
#include <Interpreters/TablesStatus.h>

#include <Compression/ICompressionCodec.h>

#include <atomic>
#include <optional>


namespace DB
{

class ClientInfo;
class Pipe;

/// Struct which represents data we are going to send for external table.
struct ExternalTableData
{
    /// Pipe of data form table;
    std::unique_ptr<Pipe> pipe;
    std::string table_name;
    /// Flag if need to stop reading.
    std::atomic_bool is_cancelled = false;
};

using ExternalTableDataPtr = std::unique_ptr<ExternalTableData>;
using ExternalTablesData = std::vector<ExternalTableDataPtr>;

class Connection;

using ConnectionPtr = std::shared_ptr<Connection>;
using Connections = std::vector<ConnectionPtr>;

using Scalars = std::map<String, Block>;


/// Packet that could be received from server.
struct Packet
{
    UInt64 type;

    Block block;
    std::unique_ptr<Exception> exception;
    std::vector<String> multistring_message;
    Progress progress;
    BlockStreamProfileInfo profile_info;

    Packet() : type(Protocol::Server::Hello) {}
};


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
    Connection(const String & host_, UInt16 port_,
        const String & default_database_,
        const String & user_, const String & password_,
        const String & client_name_ = "client",
        Protocol::Compression compression_ = Protocol::Compression::Enable,
        Protocol::Secure secure_ = Protocol::Secure::Disable,
        Poco::Timespan sync_request_timeout_ = Poco::Timespan(DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC, 0))
        :
        host(host_), port(port_), default_database(default_database_),
        user(user_), password(password_),
        client_name(client_name_),
        compression(compression_),
        secure(secure_),
        sync_request_timeout(sync_request_timeout_),
        log_wrapper(*this)
    {
        /// Don't connect immediately, only on first need.

        if (user.empty())
            user = "default";

        setDescription();
    }

    virtual ~Connection() {}

    /// Set throttler of network traffic. One throttler could be used for multiple connections to limit total traffic.
    void setThrottler(const ThrottlerPtr & throttler_)
    {
        throttler = throttler_;
    }


    /// Change default database. Changes will take effect on next reconnect.
    void setDefaultDatabase(const String & database);

    void getServerVersion(const ConnectionTimeouts & timeouts,
                          String & name,
                          UInt64 & version_major,
                          UInt64 & version_minor,
                          UInt64 & version_patch,
                          UInt64 & revision);
    UInt64 getServerRevision(const ConnectionTimeouts & timeouts);

    const String & getServerTimezone(const ConnectionTimeouts & timeouts);
    const String & getServerDisplayName(const ConnectionTimeouts & timeouts);

    /// For log and exception messages.
    const String & getDescription() const;
    const String & getHost() const;
    UInt16 getPort() const;
    const String & getDefaultDatabase() const;

    /// If last flag is true, you need to call sendExternalTablesData after.
    void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const String & query_id_ = "",
        UInt64 stage = QueryProcessingStage::Complete,
        const Settings * settings = nullptr,
        const ClientInfo * client_info = nullptr,
        bool with_pending_data = false);

    void sendCancel();
    /// Send block of data; if name is specified, server will write it to external (temporary) table of that name.
    void sendData(const Block & block, const String & name = "", bool scalar = false);
    /// Send all scalars.
    void sendScalarsData(Scalars & data);
    /// Send all contents of external (temporary) tables.
    void sendExternalTablesData(ExternalTablesData & data);

    /// Send prepared block of data (serialized and, if need, compressed), that will be read from 'input'.
    /// You could pass size of serialized/compressed block.
    void sendPreparedData(ReadBuffer & input, size_t size, const String & name = "");

    /// Check, if has data to read.
    bool poll(size_t timeout_microseconds = 0);

    /// Check, if has data in read buffer.
    bool hasReadPendingData() const;

    /// Checks if there is input data in connection and reads packet ID.
    std::optional<UInt64> checkPacket(size_t timeout_microseconds = 0);

    /// Receive packet from server.
    Packet receivePacket();

    /// If not connected yet, or if connection is broken - then connect. If cannot connect - throw an exception.
    void forceConnected(const ConnectionTimeouts & timeouts);

    bool isConnected() const { return connected; }

    TablesStatusResponse getTablesStatus(const ConnectionTimeouts & timeouts,
                                         const TablesStatusRequest & request);

    /** Disconnect.
      * This may be used, if connection is left in unsynchronised state
      *  (when someone continues to wait for something) after an exception.
      */
    void disconnect();

    size_t outBytesCount() const { return out ? out->count() : 0; }
    size_t inBytesCount() const { return in ? in->count() : 0; }

private:
    String host;
    UInt16 port;
    String default_database;
    String user;
    String password;

    /// Address is resolved during the first connection (or the following reconnects)
    /// Use it only for logging purposes
    std::optional<Poco::Net::SocketAddress> current_resolved_address;

    /// For messages in log and in exceptions.
    String description;
    void setDescription();

    /// Returns resolved address if it was resolved.
    std::optional<Poco::Net::SocketAddress> getResolvedAddress() const;

    String client_name;

    bool connected = false;

    String server_name;
    UInt64 server_version_major = 0;
    UInt64 server_version_minor = 0;
    UInt64 server_version_patch = 0;
    UInt64 server_revision = 0;
    String server_timezone;
    String server_display_name;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    std::optional<UInt64> last_input_packet_type;

    String query_id;
    Protocol::Compression compression;        /// Enable data compression for communication.
    Protocol::Secure secure;             /// Enable data encryption for communication.

    /// What compression settings to use while sending data for INSERT queries and external tables.
    CompressionCodecPtr compression_codec;

    /** If not nullptr, used to limit network traffic.
      * Only traffic for transferring blocks is accounted. Other packets don't.
      */
    ThrottlerPtr throttler;

    Poco::Timespan sync_request_timeout;

    /// From where to read query execution result.
    std::shared_ptr<ReadBuffer> maybe_compressed_in;
    BlockInputStreamPtr block_in;
    BlockInputStreamPtr block_logs_in;

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

        Poco::Logger * get()
        {
            if (!log)
                log = &Poco::Logger::get("Connection (" + parent.getDescription() + ")");

            return log;
        }

    private:
        std::atomic<Poco::Logger *> log;
        Connection & parent;
    };

    LoggerWrapper log_wrapper;

    void connect(const ConnectionTimeouts & timeouts);
    void sendHello();
    void receiveHello();
    bool ping();

    Block receiveData();
    Block receiveLogData();
    Block receiveDataImpl(BlockInputStreamPtr & stream);

    std::vector<String> receiveMultistringMessage(UInt64 msg_type);
    std::unique_ptr<Exception> receiveException();
    Progress receiveProgress();
    BlockStreamProfileInfo receiveProfileInfo();

    void initInputBuffers();
    void initBlockInput();
    void initBlockLogsInput();

    [[noreturn]] void throwUnexpectedPacket(UInt64 packet_type, const char * expected) const;
};

}
