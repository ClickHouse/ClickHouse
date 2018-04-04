#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Storages/MergeTree/AbandonableLockInZooKeeper.h>

#include <ext/scope_guard.h>
#include <pcg_random.hpp>

#include <iostream>


using namespace DB;

int main(int argc, char ** argv)
try
{
    if (argc != 3)
    {
        std::cerr << "usage: " << argv[0] << " <zookeeper_config> <path_to_table>" << std::endl;
        return 3;
    }

    ConfigProcessor processor(argv[1], false, true);
    auto config = processor.loadConfig().configuration;
    String root_path = argv[2];

    zkutil::ZooKeeper zk(*config, "zookeeper");

    String temp_path = root_path + "/temp";
    String blocks_path = root_path + "/block_numbers";

    Stopwatch total_timer;
    Stopwatch timer;

    zkutil::Stat partitions_stat;
    Strings partitions = zk.getChildren(blocks_path, &partitions_stat);

    struct LockAndPartitionID
    {
        AbandonableLockInZooKeeper lock;
        String partition_id;
    };
    std::vector<LockAndPartitionID> locks;

    std::vector<zkutil::ZooKeeper::CreateFuture> ephemeral_futures;
    for (size_t i = 0; i < partitions.size(); ++i)
    {
        String path = temp_path + "/abandonable_lock-";
        ephemeral_futures.push_back(zk.asyncCreate(path, String(), zkutil::CreateMode::EphemeralSequential));
    }
    std::vector<String> ephemerals;
    for (zkutil::ZooKeeper::CreateFuture & future : ephemeral_futures)
        ephemerals.push_back(future.get());

    auto remove_ephemerals = [&]()
    {
        std::vector<zkutil::ZooKeeper::TryRemoveFuture> futures;
        while (!ephemerals.empty())
        {
            futures.emplace_back(zk.asyncTryRemove(ephemerals.back()));
            ephemerals.pop_back();
        }
        for (auto & future : futures)
            future.get();
    };
    SCOPE_EXIT(remove_ephemerals());

    std::cerr << "Created ephemerals, elapsed: " << timer.elapsedSeconds() << std::endl;

    zkutil::Ops lock_ops;
    for (const String & partition_id : partitions)
    {
        String partition_path = blocks_path + "/" + partition_id + "/block-";
        lock_ops.push_back(std::make_shared<zkutil::Op::Create>(
                partition_path, String(), zk.getDefaultACL(), zkutil::CreateMode::PersistentSequential));
    }

    zkutil::OpResultsPtr lock_results = zk.multi(lock_ops);

    for (size_t i = 0; i < partitions.size(); ++i)
    {
        const String & partition_id = partitions[i];
        String partition_path = blocks_path + "/" + partition_id + "/block-";
        auto lock = AbandonableLockInZooKeeper::createLocked(
            partition_path, (*lock_results)[i].value, ephemerals[i], zk);
        locks.emplace_back(LockAndPartitionID{std::move(lock), partition_id});
    }

    auto abandon_locks = [&]()
    {
        std::vector<zkutil::ZooKeeper::SetFuture> futures;
        while (!locks.empty())
        {
            auto & lock = locks.back().lock;
            futures.push_back(zk.asyncSet(lock.getPath(), String(), -1));
            lock.assumeUnlocked();
            locks.pop_back();
        }
        for (auto & future : futures)
            future.get();
    };
    SCOPE_EXIT(abandon_locks());

    {
        /// Op::Create doesn't support checking the version of children, so we check with a separate operation.
        int32_t old_cversion = partitions_stat.cversion;
        zk.exists(blocks_path, &partitions_stat);
        if (partitions_stat.cversion != old_cversion)
        {
            std::cerr << "New partition added, must retry." << std::endl;
            return 1;
        }
    }

    std::cerr << "Locked, elapsed: " << timer.elapsedSeconds() << std::endl;
    for (const auto & lock_and_partition_id : locks)
        std::cout << lock_and_partition_id.partition_id << " " << lock_and_partition_id.lock.getNumber() << std::endl;
    timer.restart();

    abandon_locks();
    remove_ephemerals();
    std::cerr << "Abandoned, elapsed: " << timer.elapsedSeconds() << std::endl;

    std::cerr << "Total elapsed: " << total_timer.elapsedSeconds() << std::endl;

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << ": " << std::endl
              << e.getStackTrace().toString() << std::endl;
    throw;
}
catch (Poco::Exception & e)
{
    std::cerr << "Exception: " << e.displayText() << std::endl;
    throw;
}
catch (std::exception & e)
{
    std::cerr << "std::exception: " << e.what() << std::endl;
    throw;
}
catch (...)
{
    std::cerr << "Some exception" << std::endl;
    throw;
}
