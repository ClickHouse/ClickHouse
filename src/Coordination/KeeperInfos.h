#pragma once

#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <IO/WriteBufferFromString.h>
#include <Common/Stopwatch.h>
#include <common/types.h>


/// Contains some useful interfaces which are helpful to get keeper information.
namespace DB
{

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
    virtual ~IKeeperInfo() = default;
};

/// Keeper state machine related info
class IStateMachineInfo
{
public:
    /// last committed zxid
    [[maybe_unused]] virtual UInt64 getLastProcessedZxid() const = 0;

    virtual UInt64 getNodeCount() const = 0;
    virtual UInt64 getWatchCount() const = 0;
    virtual UInt64 getEphemeralCount() const = 0;

    /// state machine approximate data size
    virtual UInt64 getApproximateDataSize() const = 0;
    virtual std::vector<int64_t> getDeadSessions() = 0;

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
