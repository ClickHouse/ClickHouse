#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <pcg_random.hpp>

#include <iostream>
#include <thread>
#include <unistd.h>


using namespace DB;

const int MIN_DELAY = 5000;
const int MAX_DELAY = 50000;
const int FACTOR = 2;

int main(int argc, char ** argv)
try
{
    if (argc != 4)
    {
        std::cerr << "usage: " << argv[0] << " <zookeeper_config> <n_threads> <n_nodes>" << std::endl;
        return 3;
    }

    ConfigProcessor processor(argv[1], false, true);
    auto config = processor.loadConfig().configuration;
    String root_path = "/clickhouse_ztlpn/test_zk_seqnum_generation";
    String parts_path = root_path + "/parts";
    String seqnum_path = root_path + "/seq_numbers";
    String temp_path = root_path + "/temp";

    {
        zkutil::ZooKeeper zk(*config, "zookeeper");
        zk.tryRemoveRecursive(root_path);
        zk.createAncestors(root_path);
        zk.create(root_path, String(), zkutil::CreateMode::Persistent);
        zk.create(parts_path, String(), zkutil::CreateMode::Persistent);
        zk.create(seqnum_path, String(), zkutil::CreateMode::Persistent);
        zk.create(temp_path, String(), zkutil::CreateMode::Persistent);
    }


    int n_threads = std::stoi(argv[2]);
    int n_nodes = std::stoi(argv[3]);
    Poco::Event event(Poco::Event::EVENT_MANUALRESET);

    auto func = [&](int thread_no)
    {
        String log_prefix = "THREAD " + toString(thread_no) + ": ";

        pcg32_fast rng;
        rng.seed(thread_no);

        std::cerr << log_prefix << "connecting to ZK..." << std::endl;
        zkutil::ZooKeeper zk(*config, "zookeeper");
        if (!zk.exists(root_path))
        {
            std::cerr << log_prefix + "Something went wrong" << std::endl;
            return;
        }
        std::cerr << log_prefix << "connected." << std::endl;

        event.wait();

        std::cerr << log_prefix << "started" << std::endl;

        for (int i = 0; i < n_nodes; ++i)
        {
            // zk.create(parts_path + "/part-", toString(thread_no), zkutil::CreateMode::PersistentSequential);

            UInt64 delay = MIN_DELAY;
            while (true)
            {
                int32_t block_number;

                Stat stat;
                int32_t rc = zk.trySet(seqnum_path, String(), -1, &stat);
                if (rc == ZOK)
                    block_number = stat.version;
                else if (rc == ZNONODE)
                {
                    int32_t rc = zk.tryCreate(seqnum_path, String(), zkutil::CreateMode::Persistent);
                    if (rc == ZOK)
                    {
                        stat.version = 0;
                        block_number = 0;
                    }
                    else if (rc == ZNODEEXISTS)
                        continue;
                    else
                        throw zkutil::KeeperException(rc, seqnum_path);
                }
                else
                    throw zkutil::KeeperException(rc, seqnum_path);

                String node_path = parts_path + "/" + toString(block_number);

                zkutil::Ops ops;
                auto acl = zk.getDefaultACL();

                ops.emplace_back(std::make_unique<zkutil::Op::Create>(
                                     node_path, toString(thread_no), acl, zkutil::CreateMode::Persistent));
                ops.emplace_back(std::make_unique<zkutil::Op::Check>(
                                     seqnum_path, stat.version));

                rc = zk.tryMulti(ops);
                if (rc == ZOK)
                    break;
                else if (rc == ZBADVERSION)
                {
                    delay += static_cast<UInt64>(delay) * rng() / (static_cast<UInt64>(rng.max()) + 1);
                    std::cerr << log_prefix << "collision! delay: " << delay << std::endl;
                    usleep(delay);
                    delay = std::min(delay * FACTOR, MAX_DELAY);
                    continue;
                }
                else
                    throw zkutil::KeeperException(rc);
            }
        }

        std::cerr << log_prefix << "finished" << std::endl;
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < n_threads; ++i)
        threads.emplace_back(func, i);

    sleep(1);
    Stopwatch stopwatch;
    std::cout << "Starting threads..." << std::endl;
    event.set();

    for (int i = 0; i < n_threads; ++i)
        threads[i].join();

    std::cout << "Joined " << n_threads << " threads that created "
              << (n_nodes * n_threads) << " nodes in: " << stopwatch.elapsedSeconds() << "s." << std::endl;

    std::cout << "Checking correctness..." << std::endl;
    bool fail = false;

    zkutil::ZooKeeper zk(*config, "zookeeper");
    Strings part_nodes = zk.getChildren(parts_path);
    if (part_nodes.size() != static_cast<size_t>(n_threads * n_nodes))
    {
        std::cerr << "Parts size must be: " << (n_threads * n_nodes) << " not: " << part_nodes.size() << std::endl;
        fail = true;
    }

    struct Part
    {
        Int64 block_num;
        String thread_no;
        zkutil::Stat stat;
    };

    std::vector<Part> parts;
    for (const auto & node : part_nodes)
    {
        Int64 block_num = parse<Int64>(node.data(), node.length());
        Stat stat;
        String thread_no = zk.get(parts_path + "/" + node, &stat);
        parts.emplace_back(Part{block_num, thread_no, stat});
    }

    std::sort(parts.begin(), parts.end(), [](const Part & l, const Part & r) { return l.block_num < r.block_num; });

    for (size_t i = 1; i < parts.size(); ++i)
    {
        if (parts[i - 1].stat.czxid > parts[i].stat.czxid)
        {
            std::cerr
                << "Part " << parts[i - 1].block_num << " created by " << parts[i - 1].thread_no
                << " was created earlier than "
                << "Part " << parts[i].block_num << " created by " << parts[i].thread_no
                << std::endl;
            fail = true;
        }
    }

    std::cout << (fail ? "FAIL!" : "Ok.") << std::endl;
    return fail;
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
