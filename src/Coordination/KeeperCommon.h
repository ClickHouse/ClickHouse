#pragma once

#include <Common/Logger.h>

#include <cstdint>
#include <functional>
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
    V4 = 4  // 0 is not a valid digest value
};

struct KeeperDigest
{
    KeeperDigestVersion version{KeeperDigestVersion::NO_DIGEST};
    uint64_t value{0};
};

static constexpr auto KEEPER_CURRENT_DIGEST_VERSION = KeeperDigestVersion::V4;

struct KeeperResponseForSession
{
    int64_t session_id{};
    Coordination::ZooKeeperResponsePtr response;
    Coordination::ZooKeeperRequestPtr request = nullptr;
};

using KeeperResponsesForSessions = std::vector<KeeperResponseForSession>;

struct KeeperRequestForSession
{
    int64_t session_id{};
    int64_t time{0};
    Coordination::ZooKeeperRequestPtr request;
    int64_t zxid{0};
    std::optional<KeeperDigest> digest {};
    int64_t log_idx{0};
    bool use_xid_64{false};
};
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
    uint32_t data_size = 0;
    uint32_t acl_id = 0;
    int32_t version = 0;
    uint32_t num_children_and_is_ephemeral = 0;

    int64_t czxid = 0;
    int64_t mzxid = 0;
    int64_t pzxid = 0;

    int64_t ctime = 0;
    int64_t mtime = 0;

    int32_t cversion = 0;
    int32_t aversion = 0;

    int64_t ephemeral_owner_or_seq_num = 0;

    int32_t getNumChildren() const { return num_children_and_is_ephemeral >> 1; }
    bool isEphemeral() const { return (num_children_and_is_ephemeral & 1) != 0; }

    void setNumChildrenAndIsEphemeral(uint32_t num_children, bool is_ephemeral);

    void increaseNumChildren();
    void decreaseNumChildren();

    int64_t getEphemeralOwner() const { return isEphemeral() ? ephemeral_owner_or_seq_num : 0; }
    int64_t getSeqNum() const { return isEphemeral() ? 0 : ephemeral_owner_or_seq_num; }

    void setEphemeralOwner(int64_t ephemeral_owner);
    void setSeqNum(int64_t seq_num);
    void increaseSeqNum();

    uint64_t calculateDigest(std::string_view path, std::string_view data) const;

    void copyStats(const Coordination::Stat & stat);
    void setResponseStat(Coordination::Stat & response_stat) const;
};

}
