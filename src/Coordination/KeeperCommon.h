#pragma once

#include <Common/Logger.h>

#include <cstdint>
#include <functional>
#include <limits>
#include <string>
#include <string_view>


namespace Coordination
{

struct ZooKeeperRequest;
using ZooKeeperRequestPtr = std::shared_ptr<ZooKeeperRequest>;

struct ZooKeeperResponse;
using ZooKeeperResponsePtr = std::shared_ptr<ZooKeeperResponse>;

struct Stat;

}

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

bool isLocalDisk(const IDisk & disk);

class KeeperContext;
using KeeperContextPtr = std::shared_ptr<KeeperContext>;

using SessionAndTimeout = std::unordered_map<int64_t, int64_t>;

enum class KeeperDigestVersion : uint8_t
{
    NO_DIGEST = 0,
    V1 = 1,
    V2 = 2, // added system nodes that modify the digest on startup so digest from V0 is invalid
    V3 = 3, // fixed bug with casting, removed duplicate czxid usage
    V4 = 4, // 0 is not a valid digest value
    V5 = 5  // added TTL fields (destroy_time and ttl) to the node digest
};

struct KeeperDigest
{
    KeeperDigestVersion version{KeeperDigestVersion::NO_DIGEST};
    uint64_t value{0};
};

static constexpr auto KEEPER_CURRENT_DIGEST_VERSION = KeeperDigestVersion::V5;

/// One SHA1 of user:password that a session authenticated with.
struct KeeperAuthID
{
    std::string scheme;
    std::string id;

    bool operator==(const KeeperAuthID & other) const { return scheme == other.scheme && id == other.id; }
};

struct KeeperResponseForSession
{
    int64_t session_id{};
    Coordination::ZooKeeperResponsePtr response;
    Coordination::ZooKeeperRequestPtr request = nullptr;
};

using KeeperResponsesForSessions = std::vector<KeeperResponseForSession>;

/// Delivers a response whose waiter is not a per-session response callback, and returns whether it
/// took the response. Required: both dispatchers reject an empty router at construction.
using KeeperSpecialResponseRouter = std::function<bool(const KeeperResponseForSession &)>;

struct KeeperRequestForSession
{
    int64_t session_id{};
    int64_t time{0};
    Coordination::ZooKeeperRequestPtr request;
    bool use_xid_64{false};
};
using KeeperRequestsForSessions = std::vector<KeeperRequestForSession>;

struct KeeperRequestBatch
{
    std::vector<KeeperRequestForSession> requests;
    /// Which server's Keeper[Request]Dispatcher produced this batch. That dispatcher owns the
    /// sessions of all requests in the batch, so only that server produces responses to these
    /// requests when committing this batch (see produce_responses in KeeperStateMachine::commit).
    /// -1 if unknown (e.g. if parsed from log entry in old format); then all servers produce
    /// responses, and the ones that don't own the session discard them in their response threads.
    /// (TODO: the per-request-type `process` functions still construct the response object even
    ///  when it won't be used, because response construction is interleaved with applying the
    ///  committed deltas; skipping that too would require changing every `process` overload.)
    int32_t dispatcher_server_id{-1};
    /// Lower bound on last committed log entry idx. Used on startup.
    /// (This doesn't particularly need to be stored in each log entry; a single latest commit point
    ///  persisted by each server would suffice. But it's easier and probably more efficient to
    ///  piggy-back to log entries.)
    /// TODO: Use this to avoid the localLogsPreprocessed() dance on startup: read last local log
    ///       entry, take committed_log_idx from it, preprocess+commit entries up to it, preprocess
    ///       entries after it.
    int64_t committed_log_idx{0};
    /// Zxid of the first request; request i has zxid `first_zxid + i`. 0 means the leader hasn't
    /// assigned zxids to this batch yet. (Requests that don't create storage transactions, like
    /// `SessionID`, still occupy a zxid slot in the batch; those zxids are simply unused.)
    int64_t first_zxid{0};
    /// Digest of the storage state after preprocessing the whole batch. Assigned by the leader
    /// together with `first_zxid`. NO_DIGEST if disabled or not calculated yet.
    KeeperDigest digest{};
    /// Index of the log entry containing this batch. 0 if not known (yet). Not serialized.
    int64_t log_idx{0};

    int64_t getZxid(size_t request_idx) const { return first_zxid == 0 ? 0 : first_zxid + static_cast<int64_t>(request_idx); }
    int64_t getLastZxid() const { return getZxid(requests.size() - 1); }

    std::string toString() const;
};
using KeeperRequestBatchPtr = std::shared_ptr<KeeperRequestBatch>;

bool checkDigest(const KeeperDigest & first, const KeeperDigest & second);

/// If `batch.digest` and `actual` are comparable (same version, not NO_DIGEST) but different,
/// logs an error and crashes.
void assertDigest(const KeeperRequestBatch & batch, const KeeperDigest & actual, const char * operation);

inline static constexpr std::string_view tmp_keeper_file_prefix = "tmp_";

/// Parse the log index out of a snapshot file name/path. Works for both legacy
/// ("snapshot_100.bin.zstd") and unique ("snapshot_100_<uuid>.bin.zstd") names.
uint64_t getLogIdxFromSnapshotPath(const std::string & snapshot_path);

/// Canonical S3 key for a snapshot file: strips the unique suffix so every node uploads
/// the same logical index under the same key, e.g. "snapshot_100_<uuid>.bin.zstd" -> "snapshot_100.bin.zstd".
std::string getCanonicalSnapshotS3Name(const std::string & snapshot_path);

/// Narrow a setting to the int32 that most `nuraft::raft_params` fields are, warning instead of
/// wrapping when it does not fit.
int32_t getValueOrMaxInt32AndLogWarning(uint64_t value, const std::string & name, LoggerPtr log);

/// `before_file_remove_op` runs after the copy and before the source removal. Returning
/// `false` rejects the move: the source is kept, the caller cleans up the copied target.
void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<bool()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context);

/// Coarse admission classification for the memory soft limit: does this request plausibly
/// increase the amount of data Keeper stores?
///
/// This is deliberately coarse. It is evaluated in the dispatcher, before the request enters
/// Raft, where znode states are not available - so it cannot compute a real allocation delta,
/// and request byte counts have no defined relationship to storage growth. It only gets the
/// sign right. Computing the true delta needs the storage state and is tracked separately.
bool checkIfRequestIncreaseMem(const Coordination::ZooKeeperRequestPtr & request);

/// Callback invoked by KeeperDispatcher to deliver responses to clients.
/// Must be safe for concurrent invocation: setResponse (from responseThread) and
/// finishSession (from dead session cleaner) may invoke copies of the same callback
/// concurrently for the same session.
/// Returns true if the response was retained in some kind of queue and KeeperDispatcher::onResponseDeallocated will be called for it later.
/// It is valid to always return false - that just makes the queue bloat prevention less effective;
/// if you do return true, you *must* call KeeperDispatcher::onResponseDeallocated later.
using ZooKeeperResponseCallback = std::function<bool(const Coordination::ZooKeeperResponsePtr & response, Coordination::ZooKeeperRequestPtr request)>;

/// Metadata that must be stored for each znode, + data ptr and cached digest.
/// (Despite having many fields, this struct is not a kitchen sink, it doesn't have anything
///  unnecessary and is trying to be small.)
struct KeeperNodeStats
{
    /// Flags packed into ctime_and_flags.
    static constexpr uint64_t NUM_FLAGS = 3;
    static constexpr uint64_t EPHEMERAL = 1ull << 63;
    static constexpr uint64_t TTL = 1ull << 62;
    static constexpr uint64_t CONTAINER = 1ull << 61;
    static constexpr uint64_t FLAGS_MASK = EPHEMERAL | TTL | CONTAINER;
    static_assert(FLAGS_MASK == ~(~0ull >> NUM_FLAGS));

    /// ephemeralOwner value for container nodes (matches `CONTAINER_EPHEMERAL_OWNER` in ZooKeeper).
    static constexpr int64_t CONTAINER_EPHEMERAL_OWNER = INT64_MIN;

    uint32_t data_size = 0;
    uint32_t acl_id = 0;
    int32_t version = 0;
    /// Always 0 for ephemeral and TTL nodes (they can't have children).
    int32_t num_children = 0;

    int64_t czxid = 0;
    int64_t mzxid = 0;
    int64_t pzxid = 0;

    /// Upper NUM_FLAGS bits are flags, lower bits are signed ctime.
    uint64_t ctime_and_flags = 0;
    int64_t mtime = 0;

    int32_t cversion = 0;
    int32_t aversion = 0;

    /// Ephemeral owner (if isEphemeral()) or TTL (if isTTL(); in ms since mtime) or sequence number
    /// for sequentially named children (otherwise).
    int64_t ephemeral_or_seq_num_or_ttl = 0;

    bool isEphemeral() const { return (ctime_and_flags & EPHEMERAL) != 0; }
    bool isTTL() const { return (ctime_and_flags & TTL) != 0; }
    bool isContainer() const { return (ctime_and_flags & CONTAINER) != 0; }
    int64_t getCTime() const { return int64_t(ctime_and_flags << NUM_FLAGS) >> NUM_FLAGS; } // sign-extend

    int32_t getNumChildren() const { return num_children; }

    /// Sets EPHEMERAL flag in ctime_and_flags, and assigns ephemeral_or_seq_num_or_ttl.
    void makeEphemeral(int64_t ephemeral_owner);
    /// Similar for TTL.
    void makeTTL(int64_t ttl);
    void makeContainer();
    void setNumChildren(uint32_t new_num_children);
    void setCTime(int64_t ctime);

    void increaseNumChildren();
    void decreaseNumChildren();

    void setSeqNum(int64_t seq_num);
    void increaseSeqNum();

    int64_t getEphemeralOwner() const
    {
        if (isEphemeral())
            return ephemeral_or_seq_num_or_ttl;
        if (isContainer())
            return CONTAINER_EPHEMERAL_OWNER;
        return 0;
    }
    int64_t getTTL() const { return isTTL() ? ephemeral_or_seq_num_or_ttl : 0; }
    int64_t getSeqNum() const { return (isEphemeral() || isTTL()) ? 0 : ephemeral_or_seq_num_or_ttl; }

    int64_t destroyTime() const
    {
        chassert(isTTL());
        return mtime + getTTL();
    }

    uint64_t calculateDigest(std::string_view path, std::string_view data) const;

    void setResponseStat(Coordination::Stat & response_stat) const;
};

}
