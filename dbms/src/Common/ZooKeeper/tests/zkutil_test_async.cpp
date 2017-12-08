#include <Common/ZooKeeper/ZooKeeper.h>
#include <IO/ReadHelpers.h>


int main(int argc, char ** argv)
try
{
    zkutil::ZooKeeper zookeeper{"localhost:2181"};

    auto nodes = zookeeper.getChildren("/tmp");

    if (argc < 2)
    {
        std::cerr << "Usage: program num_threads\n";
        return 1;
    }

    size_t num_threads = DB::parse<size_t>(argv[1]);
    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([&]
        {
            while (true)
            {
                std::vector<zkutil::ZooKeeper::TryGetFuture> futures;
                for (auto & node : nodes)
                    futures.push_back(zookeeper.asyncTryGet("/tmp/" + node));

                for (auto & future : futures)
                    std::cerr << (future.get().value.empty() ? ',' : '.');
            }
        });
    }

    for (auto & thread : threads)
        thread.join();

    return 0;
}
catch (const Poco::Exception & e)
{
    std::cout << e.message() << std::endl;
    throw;
}
