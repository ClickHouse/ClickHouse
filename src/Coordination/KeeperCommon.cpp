#include <Coordination/KeeperCommon.h>

#include <limits>
#include <string>
#include <filesystem>
#include <thread>

#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/CoordinationSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <base/find_symbols.h>

namespace DB
{

namespace CoordinationSetting
{
    extern const CoordinationSettingsUInt64 disk_move_retries_during_init;
    extern const CoordinationSettingsUInt64 disk_move_retries_wait_ms;
}

bool isLocalDisk(const IDisk & disk)
{
    return dynamic_cast<const DiskLocal *>(&disk) != nullptr;
}

uint64_t getLogIdxFromSnapshotPath(const std::string & snapshot_path)
{
    std::filesystem::path path(snapshot_path);
    std::string filename = path.stem();
    std::vector<std::string_view> name_parts;
    splitInto<'_', '.'>(name_parts, filename);
    return parse<uint64_t>(name_parts[1]);
}

std::string getCanonicalSnapshotS3Name(const std::string & snapshot_path)
{
    const uint64_t up_to_log_idx = getLogIdxFromSnapshotPath(snapshot_path);
    return fmt::format("snapshot_{}.bin{}", up_to_log_idx, snapshot_path.ends_with(".zstd") ? ".zstd" : "");
}

int32_t getValueOrMaxInt32AndLogWarning(uint64_t value, const std::string & name, LoggerPtr log)
{
    if (value > std::numeric_limits<int32_t>::max())
    {
        LOG_WARNING(
            log,
            "Got {} value for setting '{}' which is bigger than int32_t max value, lowering value to {}.",
            value,
            name,
            std::numeric_limits<int32_t>::max());
        return std::numeric_limits<int32_t>::max();
    }

    return static_cast<int32_t>(value);
}

void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<bool()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context)
{
    /// This is the only work that frees the local disk, and the Raft write path that fills it is
    /// itself exempt from memory limit exceptions. Refusing the mover under memory pressure turns
    /// that pressure into a full disk and a fail-stop abort, so it is exempt too.
    LockMemoryExceptionInThread blocker{VariableContext::Global};

    LOG_TRACE(logger, "Moving {} to {} from disk {} to disk {}", path_from, path_to, disk_from->getName(), disk_to->getName());
    /// we use empty file with prefix tmp_ to detect incomplete copies
    /// if a copy is complete we don't care from which disk we use the same file
    /// so it's okay if a failure happens after removing of tmp file but before we remove
    /// the file from the source disk
    auto from_path = fs::path(path_from);
    auto tmp_file_name = from_path.parent_path() / (std::string{tmp_keeper_file_prefix} + from_path.filename().string());

    const auto & coordination_settings = keeper_context->getFixedCoordinationSettings();
    auto max_retries_on_init = coordination_settings[CoordinationSetting::disk_move_retries_during_init].value;
    auto retries_sleep = std::chrono::milliseconds(coordination_settings[CoordinationSetting::disk_move_retries_wait_ms]);
    auto run_with_retries = [&](const auto & op, std::string_view operation_description)
    {
        size_t retry_num = 0;
        do
        {
            try
            {
                op();
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(
                    logger,
                    fmt::format(
                        "While moving file {} to disk {} and running '{}'", path_from, disk_to->getName(), operation_description));
                std::this_thread::sleep_for(retries_sleep);
            }

            ++retry_num;
            if (keeper_context->getServerState() == KeeperContext::Phase::INIT && retry_num == max_retries_on_init)
            {
                LOG_ERROR(logger, "Operation '{}' failed too many times", operation_description);
                break;
            }
        } while (!keeper_context->isShutdownCalled());

        LOG_ERROR(
            logger,
            "Failed to run '{}' while moving file {} to disk {}",
            operation_description,
            path_from,
            disk_to->getName());
        return false;
    };

    if (!run_with_retries(
            [&]
            {
                auto buf = disk_to->writeFile(tmp_file_name);
                buf->finalize();
            },
            "creating temporary file"))
        return;

    if (!run_with_retries([&] { disk_from->copyFile(from_path, *disk_to, path_to, {}); }, "copying file"))
        return;

    if (!run_with_retries([&] { disk_to->removeFileIfExists(tmp_file_name); }, "removing temporary file"))
        return;

    if (before_file_remove_op && !before_file_remove_op())
    {
        LOG_DEBUG(logger, "Move of {} to disk {} was rejected by the caller, keeping the source file", path_from, disk_to->getName());
        return;
    }

    if (!run_with_retries([&] { disk_from->removeFileIfExists(path_from); }, "removing file from source disk"))
        return;
}

/// When this function is updated, update KEEPER_CURRENT_DIGEST_VERSION!!
uint64_t KeeperNodeStats::calculateDigest(std::string_view path, std::string_view data) const
{
    /// Must match calculateDigest in KeeperStorage.cpp (KEEPER_CURRENT_DIGEST_VERSION).
    SipHash hash;

    hash.update(path);
    if (!data.empty())
        hash.update(data);

    hash.update(czxid);
    hash.update(mzxid);
    hash.update(getCTime());
    hash.update(mtime);
    hash.update(version);
    hash.update(cversion);
    hash.update(aversion);
    hash.update(getEphemeralOwner()); // covers EPHEMERAL and CONTAINER flags
    hash.update(getNumChildren());
    hash.update(pzxid);

    hash.update(isTTL());
    if (isTTL())
        hash.update(getTTL());

    /// TODO: Hash seq num (or replace getEphemeralOwner(), getCTime(), getTTL() above with plain ephemeral_or_seq_num_or_ttl and ctime_and_flags).

    uint64_t digest = hash.get64();

    /// 0 means no calculated digest, it's not a valid digest value.
    if (digest == 0)
        digest = 1;

    return digest;
}

void KeeperNodeStats::setResponseStat(Coordination::Stat & response_stat) const
{
    response_stat.czxid = czxid;
    response_stat.mzxid = mzxid;
    response_stat.ctime = getCTime();
    response_stat.mtime = mtime;
    response_stat.version = version;
    response_stat.cversion = cversion;
    response_stat.aversion = aversion;
    response_stat.ephemeralOwner = getEphemeralOwner();
    response_stat.dataLength = static_cast<int32_t>(data_size);
    response_stat.numChildren = getNumChildren();
    response_stat.pzxid = pzxid;
}

void KeeperNodeStats::makeEphemeral(int64_t ephemeral_owner)
{
    chassert(ephemeral_owner != 0 && ephemeral_owner != CONTAINER_EPHEMERAL_OWNER);
    chassert(!isTTL() && !isContainer() && num_children == 0);
    ctime_and_flags |= EPHEMERAL;
    ephemeral_or_seq_num_or_ttl = ephemeral_owner;
}

void KeeperNodeStats::makeTTL(int64_t ttl)
{
    chassert(!isEphemeral() && !isContainer() && num_children == 0);
    ctime_and_flags |= TTL;
    ephemeral_or_seq_num_or_ttl = ttl;
}

void KeeperNodeStats::makeContainer()
{
    chassert(!isEphemeral() && !isTTL());
    ctime_and_flags |= CONTAINER;
}

void KeeperNodeStats::setNumChildren(uint32_t new_num_children)
{
    chassert(!isEphemeral() && !isTTL());
    chassert(new_num_children <= uint32_t(std::numeric_limits<int32_t>::max()));
    num_children = static_cast<int32_t>(new_num_children);
}

void KeeperNodeStats::setCTime(int64_t ctime)
{
    /// Check that ctime fits in 64 - NUM_FLAGS bits.
    chassert((int64_t(uint64_t(ctime) << NUM_FLAGS) >> NUM_FLAGS) == ctime);
    ctime_and_flags = (ctime_and_flags & FLAGS_MASK) | (uint64_t(ctime) & ~FLAGS_MASK);
}

void KeeperNodeStats::increaseNumChildren()
{
    chassert(!isEphemeral() && !isTTL());
    ++num_children;
}

void KeeperNodeStats::decreaseNumChildren()
{
    chassert(num_children > 0);
    --num_children;
}

void KeeperNodeStats::setSeqNum(int64_t seq_num)
{
    chassert(!isEphemeral() && !isTTL());
    ephemeral_or_seq_num_or_ttl = seq_num;
}

void KeeperNodeStats::increaseSeqNum()
{
    chassert(!isEphemeral() && !isTTL());
    ++ephemeral_or_seq_num_or_ttl;
}

bool checkIfRequestIncreaseMem(const Coordination::ZooKeeperRequestPtr & request)
{
    if (request->getOpNum() == Coordination::OpNum::Create
        || request->getOpNum() == Coordination::OpNum::Create2
        || request->getOpNum() == Coordination::OpNum::CreateContainer
        || request->getOpNum() == Coordination::OpNum::CreateTTL
        || request->getOpNum() == Coordination::OpNum::CreateIfNotExists)
    {
        return true;
    }

    if (request->getOpNum() == Coordination::OpNum::Set)
    {
        /// A Set cannot allocate a znode: the node must already exist, otherwise the request fails
        /// with ZNONODE and stores nothing. With empty data the amount of *stored* data can only
        /// shrink, so refusing it buys nothing - and it is how a client re-registers its session
        /// (ZooKeeper::initSession), so refusing it prevents recovery from the very condition that
        /// triggered the refusal.
        ///
        /// "Can only shrink" is about the committed state. Preprocessing an admitted Set still copies
        /// the node's current payload once into `UpdateNodeDataDelta::old_data`, so an empty Set over a
        /// large node does allocate transiently before commit frees it. That is deliberate: this is
        /// best-effort load shedding, and the alternative - refusing the write - keeps sessions from
        /// re-registering for as long as the memory event lasts.
        const auto & set_req = dynamic_cast<const Coordination::ZooKeeperSetRequest &>(*request);
        return !set_req.data.empty();
    }

    if (request->getOpNum() == Coordination::OpNum::Multi)
    {
        Coordination::ZooKeeperMultiRequest & multi_req = dynamic_cast<Coordination::ZooKeeperMultiRequest &>(*request);
        /// Add up sizes of create/set requests, subtract sizes of remove requests.
        /// This doesn't really make sense because we're interested in memory usage of znodes, not requests.
        /// But we don't know znode sizes at this point (is the Remove removing a small or big znode?),
        /// so can't do much better here. Maybe it would make sense to move this check to preprocessRequest,
        /// where we have access to the znode states.
        Int64 memory_delta = 0;
        for (const auto & sub_req : multi_req.requests)
        {
            auto sub_zk_request = std::dynamic_pointer_cast<Coordination::ZooKeeperRequest>(sub_req);
            switch (sub_zk_request->getOpNum())
            {
                case Coordination::OpNum::Create:
                case Coordination::OpNum::Create2:
                case Coordination::OpNum::CreateContainer:
                case Coordination::OpNum::CreateTTL:
                case Coordination::OpNum::CreateIfNotExists: {
                    Coordination::ZooKeeperCreateRequest & create_req
                        = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*sub_zk_request);
                    memory_delta += create_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::Set: {
                    Coordination::ZooKeeperSetRequest & set_req = dynamic_cast<Coordination::ZooKeeperSetRequest &>(*sub_zk_request);
                    /// Only the data can grow the stored size; the path already exists.
                    memory_delta += set_req.data.size();
                    break;
                }
                case Coordination::OpNum::Remove:
                case Coordination::OpNum::TryRemove: {
                    Coordination::ZooKeeperRemoveRequest & remove_req
                        = dynamic_cast<Coordination::ZooKeeperRemoveRequest &>(*sub_zk_request);
                    memory_delta -= remove_req.bytesSize();
                    break;
                }
                case Coordination::OpNum::RemoveRecursive: {
                    Coordination::ZooKeeperRemoveRecursiveRequest & remove_req
                        = dynamic_cast<Coordination::ZooKeeperRemoveRecursiveRequest &>(*sub_zk_request);
                    memory_delta -= remove_req.bytesSize();
                    break;
                }
                default:
                    break;
            }
        }
        return memory_delta > 0;
    }

    return false;
}

}
