#include <Storages/Distributed/DistributedBucketMap.h>

#include <Core/BackgroundSchedulePool.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperPathUtils.h>
#include <Common/logger_useful.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

namespace
{

constexpr size_t LOAD_RETRY_DELAY_MS = 5000;

}

DistributedBucketMap DistributedBucketMap::createEven(UInt64 buckets, size_t num_shards)
{
    if (buckets < num_shards || buckets > MAX_BUCKETS)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "The number of buckets must be at least the number of shards ({}) and at most {}, got {}",
            num_shards, MAX_BUCKETS, buckets);

    DistributedBucketMap map;
    map.buckets = buckets;
    for (size_t i = 0; i < num_shards; ++i)
        map.ranges.push_back({
            .first = buckets * i / num_shards,
            .last = buckets * (i + 1) / num_shards - 1,
            .shard_num = static_cast<UInt32>(i + 1)});

    map.validateAndBuildSlots();
    return map;
}

DistributedBucketMap DistributedBucketMap::parse(const String & data)
{
    DistributedBucketMap map;
    ReadBufferFromString in(data);

    in >> "format version: 1\n";
    in >> "buckets: " >> map.buckets >> "\n";

    while (!in.eof())
    {
        Range range;
        in >> range.first >> " " >> range.last >> " " >> range.shard_num;
        if (!in.eof())
            in >> "\n";
        map.ranges.push_back(range);
    }

    map.validateAndBuildSlots();
    return map;
}

String DistributedBucketMap::toString() const
{
    WriteBufferFromOwnString out;
    out << "format version: 1\n";
    out << "buckets: " << buckets << "\n";
    for (const auto & range : ranges)
        out << range.first << " " << range.last << " " << range.shard_num << "\n";
    return out.str();
}

void DistributedBucketMap::validateAndBuildSlots()
{
    if (buckets == 0 || buckets > MAX_BUCKETS)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The number of buckets must be from 1 to {}, got {}", MAX_BUCKETS, buckets);

    slot_to_shard.assign(buckets, 0);
    max_shard_num = 0;

    UInt64 next_bucket = 0;
    for (const auto & range : ranges)
    {
        if (range.first != next_bucket || range.last < range.first || range.last >= buckets)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Bucket range {}-{} is invalid: the ranges must go in order without gaps, the next one must start at bucket {}, "
                "and the last bucket is {}",
                range.first, range.last, next_bucket, buckets - 1);

        if (range.shard_num == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Shard numbers start from 1, got 0 for bucket range {}-{}", range.first, range.last);

        std::fill_n(slot_to_shard.data() + range.first, range.last - range.first + 1, range.shard_num - 1);
        max_shard_num = std::max(max_shard_num, range.shard_num);
        next_bucket = range.last + 1;
    }

    if (next_bucket != buckets)
        throw Exception(ErrorCodes::INCORRECT_DATA, "The bucket ranges cover {} of {} buckets", next_bucket, buckets);
}


DistributedBucketMapWatcher::DistributedBucketMapWatcher(ContextPtr context_, const StorageID & storage_id, const String & zookeeper_path_)
    : WithContext(context_->getGlobalContext())
    , full_zookeeper_path(zookeeper_path_)
    , zookeeper_name(zkutil::extractZooKeeperName(full_zookeeper_path))
    , zookeeper_path(zkutil::extractZooKeeperPath(full_zookeeper_path, /* check_starts_with_slash */ true))
    , log(getLogger(fmt::format("DistributedBucketMapWatcher ({})", storage_id.getNameForLogs())))
{
    task = getContext()->getSchedulePool()->createTask(storage_id, log->name(), [this] { run(); });
}

zkutil::ZooKeeperPtr DistributedBucketMapWatcher::getZooKeeper() const
{
    return getContext()->getDefaultOrAuxiliaryZooKeeper(zookeeper_name);
}

void DistributedBucketMapWatcher::createIfNotExists(UInt64 buckets, size_t num_shards)
{
    auto component_guard = Coordination::setCurrentComponent("DistributedBucketMapWatcher::createIfNotExists");
    auto zookeeper = getZooKeeper();

    String data;
    if (!zookeeper->tryGet(zookeeper_path, data))
    {
        if (buckets == 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bucket map {} does not exist. Set `bucket_map_buckets` to create it", full_zookeeper_path);

        zookeeper->createAncestors(zookeeper_path);
        auto code = zookeeper->tryCreate(
            zookeeper_path, DistributedBucketMap::createEven(buckets, num_shards).toString(), zkutil::CreateMode::Persistent);

        if (code == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Created bucket map {} with {} buckets for {} shards", full_zookeeper_path, buckets, num_shards);
            return;
        }

        if (code != Coordination::Error::ZNODEEXISTS)
            throw zkutil::KeeperException::fromPath(code, zookeeper_path);

        /// Another server has created the map concurrently.
        data = zookeeper->get(zookeeper_path);
    }

    auto existing_map = DistributedBucketMap::parse(data);
    if (buckets && existing_map.buckets != buckets)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Bucket map {} has {} buckets, but `bucket_map_buckets` is {}", full_zookeeper_path, existing_map.buckets, buckets);
}

void DistributedBucketMapWatcher::startup()
{
    task->activateAndSchedule();
}

void DistributedBucketMapWatcher::shutdown()
{
    task->deactivate();
}

DistributedBucketMapPtr DistributedBucketMapWatcher::get()
{
    {
        std::lock_guard lock(mutex);
        if (map)
            return map;
    }
    return load();
}

DistributedBucketMapWatcher::Status DistributedBucketMapWatcher::getStatus() const
{
    std::lock_guard lock(mutex);
    return {map, last_exception};
}

DistributedBucketMapPtr DistributedBucketMapWatcher::load()
{
    std::lock_guard load_lock(load_mutex);
    try
    {
        auto component_guard = Coordination::setCurrentComponent("DistributedBucketMapWatcher::load");
        Coordination::Stat stat;
        String data = getZooKeeper()->getWatch(zookeeper_path, &stat, task->getWatchCallback());

        auto new_map = DistributedBucketMap::parse(data);
        new_map.version = stat.version;
        auto new_map_ptr = std::make_shared<const DistributedBucketMap>(std::move(new_map));

        std::lock_guard lock(mutex);
        if (!map || map->version != new_map_ptr->version)
            LOG_INFO(log, "Loaded version {} of bucket map {}", new_map_ptr->version, full_zookeeper_path);
        map = new_map_ptr;
        last_exception = nullptr;
        return new_map_ptr;
    }
    catch (Exception & e)
    {
        e.addMessage("while loading bucket map {}", full_zookeeper_path);
        std::lock_guard lock(mutex);
        last_exception = std::current_exception();
        throw;
    }
}

void DistributedBucketMapWatcher::run()
{
    try
    {
        load();
    }
    catch (...)
    {
        tryLogCurrentException(log);
        task->scheduleAfter(LOAD_RETRY_DELAY_MS);
    }
}

}
