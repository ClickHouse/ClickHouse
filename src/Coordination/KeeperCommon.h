#pragma once

#include <Common/Logger.h>

#include <base/types.h>
#include <functional>
#include <optional>


namespace Coordination
{

struct ZooKeeperRequest;
using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperResponse;
using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;

}
namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

bool isLocalDisk(const IDisk & disk);

class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;
class KeeperSession;
using KeeperSessionPtr = std::shared_ptr<KeeperSession>;
class SessionRequest;
using SessionRequestPtr = std::shared_ptr<SessionRequest>;
using SessionRequests = std::vector<SessionRequestPtr>;

using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;

/// Callback used during new-session ID allocation (`registerNewSessionCallback` /
/// `extractNewSessionCallback`). Not used for normal session response delivery --
/// sessions use `KeeperSession::ResponseCallback` instead.
using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)>;

enum class KeeperDigestVersion : uint8_t
{
    NO_DIGEST = 0,
    V1 = 1,
    V2 = 2, // added system nodes that modify the digest on startup so digest from V0 is invalid
    V3 = 3, // fixed bug with casting, removed duplicate czxid usage
    V4 = 4  // 0 is not a valid digest value
};

struct KeeperDigest
{
    KeeperDigestVersion version{KeeperDigestVersion::NO_DIGEST};
    uint64_t value{0};
};

static constexpr auto KEEPER_CURRENT_DIGEST_VERSION = KeeperDigestVersion::V4;

/// Storage-layer response type: maps a response to its target session.
/// Used only at the `KeeperStorage::processRequest` boundary (a single request
/// can produce responses for multiple sessions — direct response + watch
/// notifications). Downstream routing to sessions is handled by `ResponseRouter`.
struct KeeperResponseForSession
{
    int64_t session_id;
    Coordination::ZooKeeperResponsePtr response;
};

using KeeperResponsesForSessions = std::vector<KeeperResponseForSession>;

/// Lightweight struct for the Raft/state-machine path.
/// Contains only the fields needed for Raft log serialization, pre_commit,
/// commit, and rollback. Does NOT carry session lifecycle state (RequestState,
/// executor, mode) — those belong to `SessionRequest` in the session layer.
struct KeeperRequestForSession
{
    int64_t session_id{0};
    int64_t time{0};
    Coordination::ZooKeeperRequestPtr request;
    int64_t zxid{0};
    std::optional<KeeperDigest> digest {};
    int64_t log_idx{0};
    bool use_xid_64{false};

    /// Timestamps set by the state machine in `pre_commit`, read in `commit`.
    uint64_t pre_commit_start_us{0};
    uint64_t pre_commit_end_us{0};
};

using KeeperRequestForSessionPtr = std::shared_ptr<KeeperRequestForSession>;
using KeeperRequestsForSessions = std::vector<KeeperRequestForSession>;

inline static constexpr std::string_view tmp_keeper_file_prefix = "tmp_";

void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<void()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context);

}
