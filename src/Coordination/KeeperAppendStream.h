#pragma once

#include "config.h"

#if USE_NURAFT

#include <libnuraft/nuraft.hxx>
#include <Common/logger_useful.h>
#include <Coordination/KeeperCommon.h>

#include <future>
#include <functional>
#include <mutex>

namespace DB
{

class KeeperServer;

/// Passes a sequence of raft entries to nuraft leader in a way that preserves order and doesn't leave gaps.
/// I.e. if an entry ends up committed (even if the success wasn't reported to the original caller),
/// all previous entries from the same stream are committed too, with lower log_idx.
/// If the leader is not on the local node, sends the entries to the leader over TCP (in a way that
/// preserves order and doesn't leave gaps).
/// If any request fails, or we lose connection to the leader, or the leader migrates, the stream
/// breaks (isBroken() == true) and doesn't accept any more requests.
/// There's no limit on the queue size (inside nuraft::rpc_client), the caller has to implement
/// its own limit on the number of in-flight requests.
class KeeperAppendStream
{
public:
    explicit KeeperAppendStream(KeeperServer * server_);

    /// If true, this stream is permanently defunct, you'll have to create a new one if you need to write more.
    bool isBroken() const;

    /// Makes isBroken() return true.
    void markAsBroken();

    /// Send requests. Not thread safe.
    /// callback(true) is called if the leader accepts entries for processing
    ///   (entries may still get lost after that, the real confirmation is commit callback).
    /// callback(false) is called if anything fails.
    /// callback may be called inline.
    /// Set to false if/when the stream is broken.
    /// It's ok to not use the callback and rely on isBroken() for errors and commit callback for successes.
    void putRequestBatch(const KeeperRequestsForSessions & requests_for_sessions, std::function<void(bool)> callback = nullptr);

private:
    KeeperServer * server;

    std::optional<uint64_t> term; // if set, we're passing requests to local leader
    nuraft::ptr<nuraft::rpc_client> client; // if set, we're sending requests to remote leader

    std::shared_ptr<std::atomic<bool>> is_broken = std::make_shared<std::atomic<bool>>(false);
};

}

#endif
