#include <Coordination/KeeperCommon.h>

#include <limits>
#include <string>
#include <filesystem>
#include <thread>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Common/ZooKeeper/IKeeper.h>
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

void moveFileBetweenDisks(
    DiskPtr disk_from,
    const std::string & path_from,
    DiskPtr disk_to,
    const std::string & path_to,
    std::function<bool()> before_file_remove_op,
    LoggerPtr logger,
    const KeeperContextPtr & keeper_context)
{
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
    hash.update(getEphemeralOwner());
    hash.update(getNumChildren());
    hash.update(pzxid);

    hash.update(isTTL());
    if (isTTL())
        hash.update(getTTL());

    uint64_t digest = hash.get64();

    /// 0 means no calculated digest, it's not a valid digest value.
    if (digest == 0)
        digest = 1;

    return digest;
}

void KeeperNodeStats::copyStats(const Coordination::Stat & stat)
{
    czxid = stat.czxid;
    mzxid = stat.mzxid;
    pzxid = stat.pzxid;

    mtime = stat.mtime;
    ctime_and_flags = 0;
    setCTime(stat.ctime);

    version = stat.version;
    cversion = stat.cversion;
    aversion = stat.aversion;

    data_size = stat.dataLength;

    num_children = 0;
    ephemeral_or_seq_num_or_ttl = 0;
    if (stat.ephemeralOwner == 0)
        setNumChildren(stat.numChildren);
    else
        makeEphemeral(stat.ephemeralOwner);
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
    chassert(ephemeral_owner != 0);
    chassert(!isTTL() && num_children == 0);
    ctime_and_flags |= EPHEMERAL;
    ephemeral_or_seq_num_or_ttl = ephemeral_owner;
}

void KeeperNodeStats::makeTTL(int64_t ttl)
{
    chassert(!isEphemeral() && num_children == 0);
    ctime_and_flags |= TTL;
    ephemeral_or_seq_num_or_ttl = ttl;
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

}
