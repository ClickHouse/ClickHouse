#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <ext/scope_guard.h>

#include <iostream>


using namespace DB;

/// This test is useful for assessing the performance of getting the numbers of all currently committing
/// blocks from ZooKeeper. This is needed to select merges without checking that all block numbers between
/// parts have been abandoned (see DB::ReplicatedMergeTreeMergePredicate for details).
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
    String zookeeper_path = argv[2];

    auto zookeeper = std::make_shared<zkutil::ZooKeeper>(*config, "zookeeper");

    std::unordered_map<String, std::set<Int64>> current_inserts;

    Stopwatch total;
    Stopwatch stage;
    /// Load current inserts
    std::unordered_set<String> lock_holder_paths;
    for (const String & entry : zookeeper->getChildren(zookeeper_path + "/temp"))
    {
        if (startsWith(entry, "abandonable_lock-"))
            lock_holder_paths.insert(zookeeper_path + "/temp/" + entry);
    }
    std::cerr << "Stage 1 (get lock holders): " << lock_holder_paths.size()
              << " lock holders, elapsed: " << stage.elapsedSeconds() << "s." << std::endl;
    stage.restart();

    if (!lock_holder_paths.empty())
    {
        Strings partitions = zookeeper->getChildren(zookeeper_path + "/block_numbers");
        std::cerr << "Stage 2 (get partitions): " << partitions.size()
                  << " partitions, elapsed: " << stage.elapsedSeconds() << "s." << std::endl;
        stage.restart();

        std::vector<std::future<Coordination::ListResponse>> lock_futures;
        for (const String & partition : partitions)
            lock_futures.push_back(zookeeper->asyncGetChildren(zookeeper_path + "/block_numbers/" + partition));

        struct BlockInfo
        {
            String partition;
            Int64 number;
            String zk_path;
            std::future<Coordination::GetResponse> contents_future;
        };

        std::vector<BlockInfo> block_infos;
        for (size_t i = 0; i < partitions.size(); ++i)
        {
            Strings partition_block_numbers = lock_futures[i].get().names;
            for (const String & entry : partition_block_numbers)
            {
                Int64 block_number = parse<Int64>(entry.substr(strlen("block-")));
                String zk_path = zookeeper_path + "/block_numbers/" + partitions[i] + "/" + entry;
                block_infos.push_back(
                    BlockInfo{partitions[i], block_number, zk_path, zookeeper->asyncTryGet(zk_path)});
            }
        }
        std::cerr << "Stage 3 (get block numbers): " << block_infos.size()
                  << " block numbers, elapsed: " << stage.elapsedSeconds() << "s." << std::endl;
        stage.restart();

        size_t total_count = 0;
        for (BlockInfo & block : block_infos)
        {
            Coordination::GetResponse resp = block.contents_future.get();
            if (resp.error == Coordination::Error::ZOK && lock_holder_paths.count(resp.data))
            {
                ++total_count;
                current_inserts[block.partition].insert(block.number);
            }
        }
        std::cerr << "Stage 4 (get block number contents): " << total_count
                  << " current_inserts, elapsed: " << stage.elapsedSeconds() << "s." << std::endl;
        stage.restart();
    }

    std::cerr << "Total elapsed: " << total.elapsedSeconds() << "s." << std::endl;

    for (const auto & kv : current_inserts)
    {
        std::cout << kv.first << ": ";
        for (Int64 num : kv.second)
            std::cout << num << ", ";
        std::cout << std::endl;
    }

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << ": " << std::endl
              << e.getStackTraceString() << std::endl;
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
