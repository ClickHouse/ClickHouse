#include <Storages/MergeTree/EphemeralLockInZooKeeper.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/logger_useful.h>
#include <base/types.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

EphemeralLockInZooKeeper::EphemeralLockInZooKeeper(const String & path_prefix_, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const String & path_, const String & conflict_path_)
    : zookeeper(zookeeper_), path_prefix(path_prefix_), path(path_), conflict_path(conflict_path_)
{
    if (conflict_path.empty() && path.size() <= path_prefix.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Name of the main node is shorter than prefix.");
}

template <typename T>
std::optional<EphemeralLockInZooKeeper> createEphemeralLockInZooKeeper(
    const String & path_prefix_, const String & temp_path, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const T & deduplication_path)
{
    static constexpr bool async_insert = std::is_same_v<T, std::vector<String>>;

    String path;

    if (deduplication_path.empty())
    {
        String holder_path = temp_path + "/" + EphemeralLockInZooKeeper::LEGACY_LOCK_OTHER;
        path = zookeeper_->create(path_prefix_, holder_path, zkutil::CreateMode::EphemeralSequential);
    }
    else
    {
        String holder_path = temp_path + "/" + EphemeralLockInZooKeeper::LEGACY_LOCK_INSERT;

        /// Check for duplicates in advance, to avoid superfluous block numbers allocation
        Coordination::Requests ops;
        if constexpr (async_insert)
        {
            for (const auto & single_dedup_path : deduplication_path)
                zkutil::addCheckNotExistsRequest(ops, *zookeeper_, single_dedup_path);
        }
        else
        {
            zkutil::addCheckNotExistsRequest(ops, *zookeeper_, deduplication_path);
        }

        auto deduplication_path_ops_size = ops.size();

        ops.emplace_back(zkutil::makeCreateRequest(path_prefix_, holder_path, zkutil::CreateMode::EphemeralSequential));
        Coordination::Responses responses;
        Coordination::Error e = zookeeper_->tryMulti(ops, responses);
        if (e == Coordination::Error::ZNODEEXISTS)
        {
            if constexpr (async_insert)
            {
                auto failed_idx = zkutil::getFailedOpIndex(Coordination::Error::ZNODEEXISTS, responses);

                if (failed_idx < deduplication_path_ops_size)
                {
                    const String & failed_op_path = ops[failed_idx]->getPath();
                    LOG_DEBUG(
                        getLogger("createEphemeralLockInZooKeeper"),
                        "Deduplication path already exists: deduplication_path={}",
                        failed_op_path);
                    return EphemeralLockInZooKeeper{"", nullptr, "", failed_op_path};
                }
            }
            else if (responses[0]->error == Coordination::Error::ZNODEEXISTS)
            {
                LOG_DEBUG(
                    getLogger("createEphemeralLockInZooKeeper"),
                    "Deduplication path already exists: deduplication_path={}",
                    deduplication_path);
                return {};
            }
        }

        if (e != Coordination::Error::ZOK)
        {
            zkutil::KeeperMultiException::check(e, ops, responses); // This should always throw the proper exception
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unable to handle error {} when acquiring ephemeral lock in ZK", toString(e));
        }

        path = dynamic_cast<const Coordination::CreateResponse *>(responses.back().get())->path_created;
    }

    return EphemeralLockInZooKeeper{path_prefix_, zookeeper_, path};
}

void EphemeralLockInZooKeeper::unlock()
{
    Coordination::Requests ops;
    getUnlockOp(ops);
    zookeeper->multi(ops);
    zookeeper = nullptr;
}

void EphemeralLockInZooKeeper::getUnlockOp(Coordination::Requests & ops)
{
    checkCreated();
    ops.emplace_back(zkutil::makeRemoveRequest(path, -1));
}

EphemeralLockInZooKeeper::~EphemeralLockInZooKeeper()
{
    if (!isLocked())
        return;

    try
    {
        unlock();
    }
    catch (const zkutil::KeeperException & e)
    {
        if (Coordination::isHardwareError(e.code))
            LOG_DEBUG(
                getLogger("EphemeralLockInZooKeeper"),
                "ZooKeeper communication error during unlock: code={} message='{}'",
                e.code,
                e.message());
        else if (e.code == Coordination::Error::ZNONODE)
            /// To avoid additional round-trip for unlocking,
            /// ephemeral node can be deleted explicitly as part of another multi op request to ZK
            /// and marked as such via assumeUnlocked() if we got successful response.
            /// But it's possible that the multi op request can be executed on server side, and client will not get response due to network issue.
            /// In such case, assumeUnlocked() will not be called, so we'll get ZNONODE error here since the noded is already deleted
            LOG_DEBUG(
                getLogger("EphemeralLockInZooKeeper"),
                "ZooKeeper node was already deleted: code={} message={}",
                e.code,
                e.message());
        else
            tryLogCurrentException("EphemeralLockInZooKeeper");
    }
    catch (...)
    {
        tryLogCurrentException("EphemeralLockInZooKeeper");
    }
}


EphemeralLocksInAllPartitions::EphemeralLocksInAllPartitions(
    const String & block_numbers_path, const String & path_prefix, const String & temp_path,
    zkutil::ZooKeeper & zookeeper_)
    : zookeeper(&zookeeper_)
{
    String holder_path = temp_path + "/" + EphemeralLockInZooKeeper::LEGACY_LOCK_OTHER;
    while (true)
    {
        Coordination::Stat partitions_stat;
        Strings partitions = zookeeper->getChildren(block_numbers_path, &partitions_stat);

        Coordination::Requests lock_ops;
        for (const auto & partition : partitions)
        {
            String partition_path_prefix = block_numbers_path + "/" + partition + "/" + path_prefix;
            lock_ops.push_back(zkutil::makeCreateRequest(
                    partition_path_prefix, holder_path, zkutil::CreateMode::EphemeralSequential));
        }
        lock_ops.push_back(zkutil::makeCheckRequest(block_numbers_path, partitions_stat.version));

        Coordination::Responses lock_responses;
        Coordination::Error rc = zookeeper->tryMulti(lock_ops, lock_responses);
        if (rc == Coordination::Error::ZBADVERSION)
        {
            LOG_TRACE(getLogger("EphemeralLocksInAllPartitions"), "Someone has inserted a block in a new partition while we were creating locks. Retry.");
            continue;
        }
        if (rc != Coordination::Error::ZOK)
            throw Coordination::Exception(rc);

        for (size_t i = 0; i < partitions.size(); ++i)
        {
            size_t prefix_size = block_numbers_path.size() + 1 + partitions[i].size() + 1 + path_prefix.size();
            const String & path = dynamic_cast<const Coordination::CreateResponse &>(*lock_responses[i]).path_created;
            if (path.size() <= prefix_size)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Name of the sequential node is shorter than prefix.");

            UInt64 number = parse<UInt64>(path.c_str() + prefix_size, path.size() - prefix_size);
            locks.push_back(LockInfo{path, partitions[i], number});
        }

        return;
    }
}

void EphemeralLocksInAllPartitions::unlock()
{
    if (!zookeeper)
        return;

    std::vector<zkutil::ZooKeeper::FutureRemove> futures;
    futures.reserve(locks.size());
    for (const auto & lock : locks)
    {
        futures.push_back(zookeeper->asyncRemove(lock.path));
    }

    for (auto & future : futures)
        future.get();

    locks.clear();
    zookeeper = nullptr;
}

EphemeralLocksInAllPartitions::~EphemeralLocksInAllPartitions()
{
    try
    {
        unlock();
    }
    catch (...)
    {
        tryLogCurrentException("~EphemeralLocksInAllPartitions");
    }
}

template std::optional<EphemeralLockInZooKeeper> createEphemeralLockInZooKeeper<String>(
    const String & path_prefix_, const String & temp_path, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const String & deduplication_path);

template std::optional<EphemeralLockInZooKeeper> createEphemeralLockInZooKeeper<std::vector<String>>(
    const String & path_prefix_, const String & temp_path, const ZooKeeperWithFaultInjectionPtr & zookeeper_, const std::vector<String> & deduplication_path);

}
