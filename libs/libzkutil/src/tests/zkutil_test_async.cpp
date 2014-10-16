#include <zkutil/ZooKeeper.h>


int main(int argc, char ** argv)
try
{
	zkutil::ZooKeeper zookeeper{"localhost:2181"};

	auto task = zookeeper.asyncGet(argc <= 1 ? "/" : argv[1]);
	auto future = task->get_future();
	auto res = future.get();

	std::cerr << res.value << ", " << res.stat.numChildren << '\n';
	return 0;
}
catch (const Poco::Exception & e)
{
	std::cout << e.message() << std::endl;
	throw;
}
