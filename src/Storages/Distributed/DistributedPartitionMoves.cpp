#include <Storages/Distributed/DistributedPartitionMoves.h>

#include <Core/BackgroundSchedulePool.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperPathUtils.h>
#include <Common/logger_useful.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

namespace
{

constexpr size_t LOAD_RETRY_DELAY_MS = 5000;

}

DistributedPartitionMoves DistributedPartitionMoves::parse(const String & data)
{
    DistributedPartitionMoves result;
    ReadBufferFromString in(data);

    in >> "format version: 1\n";

    std::unordered_set<String> partition_ids;
    while (!in.eof())
    {
        Move move;
        String read_from;
        readStringUntilWhitespace(move.partition_id, in);
        in >> " " >> move.source_shard_num >> " " >> move.destination_shard_num >> " ";
        readStringUntilNewlineInto(read_from, in);
        if (!in.eof())
            in >> "\n";

        if (move.partition_id.empty())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Partition ID of a move is empty");

        if (!partition_ids.insert(move.partition_id).second)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Partition {} is moved more than once", move.partition_id);

        if (move.source_shard_num == 0 || move.destination_shard_num == 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Shard numbers start from 1, got 0 in the move of partition {}", move.partition_id);

        if (move.source_shard_num == move.destination_shard_num)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Partition {} is moved from shard {} to the same shard", move.partition_id, move.source_shard_num);

        if (read_from == "source")
            move.read_from = ReadFrom::Source;
        else if (read_from == "destination")
            move.read_from = ReadFrom::Destination;
        else
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "The copy to read in the move of partition {} must be `source` or `destination`, got `{}`", move.partition_id, read_from);

        UInt32 shard_to_skip = move.read_from == ReadFrom::Source ? move.destination_shard_num : move.source_shard_num;
        result.partitions_to_skip_by_shard[shard_to_skip].push_back(move.partition_id);
        result.max_shard_num = std::max({result.max_shard_num, move.source_shard_num, move.destination_shard_num});
        result.moves.push_back(std::move(move));
    }

    return result;
}


DistributedPartitionMovesWatcher::DistributedPartitionMovesWatcher(
    ContextPtr context_, const StorageID & storage_id, const String & zookeeper_path_)
    : WithContext(context_->getGlobalContext())
    , full_zookeeper_path(zookeeper_path_)
    , zookeeper_name(zkutil::extractZooKeeperName(full_zookeeper_path))
    , zookeeper_path(zkutil::extractZooKeeperPath(full_zookeeper_path, /* check_starts_with_slash */ true))
    , log(getLogger(fmt::format("DistributedPartitionMovesWatcher ({})", storage_id.getNameForLogs())))
{
    task = getContext()->getSchedulePool()->createTask(storage_id, log->name(), [this] { run(); });
}

zkutil::ZooKeeperPtr DistributedPartitionMovesWatcher::getZooKeeper() const
{
    return getContext()->getDefaultOrAuxiliaryZooKeeper(zookeeper_name);
}

void DistributedPartitionMovesWatcher::createIfNotExists()
{
    auto component_guard = Coordination::setCurrentComponent("DistributedPartitionMovesWatcher::createIfNotExists");
    auto zookeeper = getZooKeeper();

    String data;
    if (zookeeper->tryGet(zookeeper_path, data))
    {
        DistributedPartitionMoves::parse(data);
        return;
    }

    zookeeper->createAncestors(zookeeper_path);
    auto code = zookeeper->tryCreate(zookeeper_path, DistributedPartitionMoves::EMPTY, zkutil::CreateMode::Persistent);
    if (code == Coordination::Error::ZOK)
        LOG_INFO(log, "Created partition moves {}", full_zookeeper_path);
    else if (code != Coordination::Error::ZNODEEXISTS)
        throw zkutil::KeeperException::fromPath(code, zookeeper_path);
}

void DistributedPartitionMovesWatcher::startup()
{
    task->activateAndSchedule();
}

void DistributedPartitionMovesWatcher::shutdown()
{
    task->deactivate();
}

DistributedPartitionMovesPtr DistributedPartitionMovesWatcher::get()
{
    {
        std::lock_guard lock(mutex);
        if (moves)
            return moves;
    }
    return load();
}

DistributedPartitionMovesWatcher::Status DistributedPartitionMovesWatcher::getStatus() const
{
    std::lock_guard lock(mutex);
    return {moves, last_exception};
}

DistributedPartitionMovesPtr DistributedPartitionMovesWatcher::load()
{
    std::lock_guard load_lock(load_mutex);
    try
    {
        auto component_guard = Coordination::setCurrentComponent("DistributedPartitionMovesWatcher::load");
        Coordination::Stat stat;
        String data = getZooKeeper()->getWatch(zookeeper_path, &stat, task->getWatchCallback());

        auto new_moves = DistributedPartitionMoves::parse(data);
        new_moves.version = stat.version;
        auto new_moves_ptr = std::make_shared<const DistributedPartitionMoves>(std::move(new_moves));

        std::lock_guard lock(mutex);
        if (!moves || moves->version != new_moves_ptr->version)
            LOG_INFO(log, "Loaded version {} of partition moves {} with {} moves",
                new_moves_ptr->version, full_zookeeper_path, new_moves_ptr->moves.size());
        moves = new_moves_ptr;
        last_exception = nullptr;
        return new_moves_ptr;
    }
    catch (Exception & e)
    {
        e.addMessage("while loading partition moves {}", full_zookeeper_path);
        std::lock_guard lock(mutex);
        last_exception = std::current_exception();
        throw;
    }
}

void DistributedPartitionMovesWatcher::run()
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
