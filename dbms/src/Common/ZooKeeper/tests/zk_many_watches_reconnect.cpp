#include <Common/Config/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Poco/Event.h>
#include <iostream>

/// A tool for reproducing https://issues.apache.org/jira/browse/ZOOKEEPER-706
/// Original libzookeeper can't reconnect the session if the length of SET_WATCHES message
/// exceeeds jute.maxbuffer (0xfffff by default).
/// This happens when the number of watches exceeds ~29000.
///
/// Session reconnect can be caused by forbidding packets to the current zookeeper server, e.g.
/// sudo ip6tables -A OUTPUT -d mtzoo01it.haze.yandex.net -j REJECT

const size_t N_THREADS = 100;

int main(int argc, char ** argv)
{
    try
    {
        if (argc != 3)
        {
            std::cerr << "usage: " << argv[0] << " <zookeeper_config> <number_of_watches>" << std::endl;
            return 3;
        }

        ConfigProcessor processor(argv[1], false, true);
        auto config = processor.loadConfig().configuration;
        zkutil::ZooKeeper zk(*config, "zookeeper");
        zkutil::EventPtr watch = std::make_shared<Poco::Event>();

        /// NOTE: setting watches in multiple threads because doing it in a single thread is too slow.
        size_t watches_per_thread = std::stoull(argv[2]) / N_THREADS;
        std::vector<std::thread> threads;
        for (size_t i_thread = 0; i_thread < N_THREADS; ++i_thread)
        {
            threads.emplace_back([&, i_thread]
                {
                    for (size_t i = 0; i < watches_per_thread; ++i)
                        zk.exists("/clickhouse/nonexistent_node" + std::to_string(i * N_THREADS + i_thread), nullptr, watch);
                });
        }
        for (size_t i_thread = 0; i_thread < N_THREADS; ++i_thread)
            threads[i_thread].join();

        while (true)
        {
            std::cerr << "WAITING..." << std::endl;
            sleep(10);
        }
    }
    catch (Poco::Exception & e)
    {
        std::cerr << "Exception: " << e.displayText() << std::endl;
        return 1;
    }
    catch (std::exception & e)
    {
        std::cerr << "std::exception: " << e.what() << std::endl;
        return 3;
    }
    catch (...)
    {
        std::cerr << "Some exception" << std::endl;
        return 2;
    }

    return 0;
}
