#pragma once

#include <Common/Throttler.h>
#include <Core/Types.h>
#include <Core/QueryProcessingStage.h>
#include <Core/Block.h>
#include <Core/Protocol.h>

#include <QueryPipeline/ProfileInfo.h>

#include <QueryPipeline/Pipe.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/Progress.h>

#include <Storages/MergeTree/RequestResponse.h>

#include <boost/noncopyable.hpp>

#include <optional>
#include <vector>
#include <memory>
#include <string>

namespace DB
{

class ClientInfo;

/// Packet that could be received from server.
struct Packet
{
    UInt64 type;

    Block block;
    std::unique_ptr<Exception> exception;
    std::vector<String> multistring_message;
    Progress progress;
    ProfileInfo profile_info;
    std::vector<UUID> part_uuids;

    /// The part of parallel replicas protocol
    std::optional<InitialAllRangesAnnouncement> announcement;
    std::optional<ParallelReadRequest> request;

    std::string server_timezone;

    Packet() : type(Protocol::Server::Hello) {}
};


/// Struct which represents data we are going to send for external table.
struct ExternalTableData
{
    /// Pipe of data form table;
    std::unique_ptr<QueryPipelineBuilder> pipe;
    std::string table_name;
    std::function<std::unique_ptr<QueryPipelineBuilder>()> creating_pipe_callback;
    /// Flag if need to stop reading.
    std::atomic_bool is_cancelled = false;
};

using ExternalTableDataPtr = std::unique_ptr<ExternalTableData>;
using ExternalTablesData = std::vector<ExternalTableDataPtr>;


class IServerConnection : boost::noncopyable
{
public:
    virtual ~IServerConnection() = default;

    enum class Type : uint8_t
    {
        SERVER,
        LOCAL
    };

    virtual Type getConnectionType() const = 0;

    virtual void setDefaultDatabase(const String & database) = 0;

    virtual void getServerVersion(
            const ConnectionTimeouts & timeouts, String & name,
            UInt64 & version_major, UInt64 & version_minor,
            UInt64 & version_patch, UInt64 & revision) = 0;

    virtual UInt64 getServerRevision(const ConnectionTimeouts & timeouts) = 0;

    virtual const String & getServerTimezone(const ConnectionTimeouts & timeouts) = 0;
    virtual const String & getServerDisplayName(const ConnectionTimeouts & timeouts) = 0;

    virtual const String & getDescription(bool with_extra = false) const = 0;  /// NOLINT

    virtual std::vector<std::pair<String, String>> getPasswordComplexityRules() const = 0;

    /// If last flag is true, you need to call sendExternalTablesData after.
    virtual void sendQuery(
        const ConnectionTimeouts & timeouts,
        const String & query,
        const NameToNameMap & query_parameters,
        const String & query_id_,
        UInt64 stage,
        const Settings * settings,
        const ClientInfo * client_info,
        bool with_pending_data,
        std::function<void(const Progress &)> process_progress_callback) = 0;

    virtual void sendCancel() = 0;

    /// Send block of data; if name is specified, server will write it to external (temporary) table of that name.
    virtual void sendData(const Block & block, const String & name, bool scalar) = 0;

    /// Send all contents of external (temporary) tables.
    virtual void sendExternalTablesData(ExternalTablesData & data) = 0;

    virtual void sendMergeTreeReadTaskResponse(const ParallelReadResponse & response) = 0;

    /// Check, if has data to read.
    virtual bool poll(size_t timeout_microseconds) = 0;

    /// Check, if has data in read buffer.
    virtual bool hasReadPendingData() const = 0;

    /// Checks if there is input data in connection and reads packet ID.
    virtual std::optional<UInt64> checkPacket(size_t timeout_microseconds) = 0;

    /// Receive packet from server.
    virtual Packet receivePacket() = 0;

    /// If not connected yet, or if connection is broken - then connect. If cannot connect - throw an exception.
    virtual void forceConnected(const ConnectionTimeouts & timeouts) = 0;

    virtual bool isConnected() const = 0;

    /// Check if connection is still active with ping request.
    virtual bool checkConnected(const ConnectionTimeouts & /*timeouts*/) = 0;

    /** Disconnect.
      * This may be used, if connection is left in unsynchronised state
      *  (when someone continues to wait for something) after an exception.
      */
    virtual void disconnect() = 0;

    /// Set throttler of network traffic. One throttler could be used for multiple connections to limit total traffic.
    virtual void setThrottler(const ThrottlerPtr & throttler_) = 0;
};

using ServerConnectionPtr = std::unique_ptr<IServerConnection>;

}
