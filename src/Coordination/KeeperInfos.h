#pragma once

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <Coordination/KeeperStats.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/DateTime.h>
#include <Common/Stopwatch.h>


/// Contains some useful interfaces which are helpful to get keeper information.
namespace DB
{

struct LastOp
{
private:
    String last_op{"NA"};
    Int64 last_cxid{-1};
    Int64 last_zxid{-1};
    Int64 last_response_time{0};
    Int64 last_latency{0};

public:
    inline void update(String last_op_, Int64 last_cxid_, Int64 last_zxid_, Int64 last_response_time_, Int64 last_latency_)
    {
        last_op = last_op_;
        last_cxid = last_cxid_;
        last_zxid = last_zxid_;
        last_response_time = last_response_time_;
        last_latency = last_latency_;
    }

    inline LastOp clone() { return *this; }

    inline void reset()
    {
        last_op = "NA";
        last_cxid = -1;
        last_zxid = -1;
        last_response_time = 0;
        last_latency = 0;
    }

    inline const String & getLastOp() const { return last_op; }
    inline Int64 getLastCxid() const { return last_cxid; }
    inline Int64 getLastZxid() const { return last_zxid; }
    inline Int64 getLastResponseTime() const { return last_response_time; }
    inline Int64 getLastLatency() const { return last_latency; }
};

class IConnectionInfo
{
public:
    virtual Int64 getPacketsReceived() const = 0;
    virtual Int64 getPacketsSent() const = 0;
    virtual Int64 getSessionId() const = 0;
    virtual Int64 getSessionTimeout() const = 0;
    /// startup time
    virtual Poco::Timestamp getEstablished() const = 0;
    virtual LastOp getLastOp() const = 0;
    virtual KeeperStatsPtr getSessionStats() const = 0;

    virtual ~IConnectionInfo() = default;
};

/// Keeper server related information
class IKeeperInfo
{
public:
    virtual bool isLeader() const = 0;
    virtual bool hasLeader() const = 0;

    /// "leader", "follower", "observer"
    virtual String getRole() const = 0;

    /// number alive connections of this node
    virtual UInt64 getNumAliveConnections() const = 0;

    /// number of requests in queue
    virtual UInt64 getOutstandingRequests() const = 0;

    /// log dir size
    virtual UInt64 getDataDirSize() const = 0;
    /// snapshot dir size
    virtual UInt64 getSnapDirSize() const = 0;

    /// dump session list connection to the node
    virtual void dumpSessions(WriteBufferFromOwnString & buf) const = 0;

    virtual ~IKeeperInfo() = default;
};

/// Keeper state machine related info
class IStateMachineInfo
{
public:
    /// last committed zxid
    virtual UInt64 getLastProcessedZxid() const = 0;

    virtual UInt64 getNodeCount() const = 0;
    virtual UInt64 getWatchCount() const = 0;
    virtual UInt64 getWatchPathCount() const = 0;
    /// session count who has ephemeral nodes
    virtual UInt64 getEphemeralCount() const = 0;
    virtual UInt64 getEphemeralNodeCount() const = 0;

    /// state machine approximate data size
    virtual UInt64 getApproximateDataSize() const = 0;
    virtual std::vector<int64_t> getDeadSessions() = 0;

    virtual void dumpWatches(WriteBufferFromOwnString & buf) const = 0;
    virtual void dumpWatchesByPath(WriteBufferFromOwnString & buf) const = 0;
    virtual void dumpEphemerals(WriteBufferFromOwnString & buf) const = 0;

    virtual ~IStateMachineInfo() = default;
};

/// Raft related info
class IRaftInfo
{
public:
    virtual bool isLeader() const = 0;

    virtual bool isLeaderAlive() const = 0;

    /// server role ignore zookeeper state "read-only" and "standalone"
    virtual String getRole() const = 0;

    /// @return follower count if node is not leader return 0
    virtual UInt64 getFollowerCount() const = 0;

    /// @return synced follower count if node is not leader return 0
    virtual UInt64 getSyncedFollowerCount() const = 0;

    virtual ~IRaftInfo() = default;
};

}
