#include <zkutil/ZooKeeper.h>


int main(int argc, char ** argv)
try
{
	zkutil::ZooKeeper zookeeper{"localhost:2181"};

	auto nodes = zookeeper.getChildren("/tmp");

	std::vector<std::thread> threads;
	for (size_t i = 0; i < 4; ++i)
	{
		threads.emplace_back([&]
		{
			while (true)
			{
				std::vector<zkutil::ZooKeeper::GetFuture> futures;
				for (auto & node : nodes)
					futures.push_back(zookeeper.asyncGet("/tmp/" + node));

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
