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

/// This test is useful for assessing the performance of acquiring block numbers in all partitions (and there
/// can be ~1000 of them). This is needed when creating a mutation entry for a ReplicatedMergeTree table.
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

    EphemeralLocksInAllPartitions locks(blocks_path, "test_lock-", temp_path, zk);

    std::cerr << "Locked, elapsed: " << timer.elapsedSeconds() << std::endl;
    for (const auto & lock : locks.getLocks())
        std::cout << lock.partition_id << " " << lock.number << std::endl;
    timer.restart();

    locks.unlock();
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
