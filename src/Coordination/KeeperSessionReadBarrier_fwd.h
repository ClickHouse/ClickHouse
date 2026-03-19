#pragma once

#include <Coordination/KeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>

#include <cstdint>
#include <vector>


namespace DB
{

/// A read request that was deferred until a pending write commits or rolls back.
/// Shared between the per-session barrier and the legacy `read_request_queue`.
struct DeferredReadRequest
{
    uint64_t deferred_wait_start_us = 0;
    KeeperRequestForSession request_for_session;
};
using DeferredReadRequests = std::vector<DeferredReadRequest>;

/// Result of attempting to defer a read.
enum class DeferReadResult
{
    /// No pending writes for this session; execute immediately.
    Immediate,
    /// Read was deferred; will be collected on write resolution.
    Deferred,
    /// Session is degraded or limit exceeded; caller should reject.
    Rejected,
};

/// Result of resolving a committed write.
struct CommitResolveResult
{
    DeferredReadRequests reads_to_execute;
    DeferredReadRequests reads_to_fail;
};

/// Returns true if this operation participates in per-session barrier tracking.
/// Heartbeats and session-id requests are excluded.
inline bool isPerSessionBarrierOp(Coordination::OpNum op_num)
{
    return op_num != Coordination::OpNum::Heartbeat && op_num != Coordination::OpNum::SessionID;
}

class KeeperSessionReadBarrier;

}
