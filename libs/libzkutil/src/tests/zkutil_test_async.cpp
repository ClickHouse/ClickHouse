#include <zkutil/ZooKeeper.h>


int main(int argc, char ** argv)
try
{
	zkutil::ZooKeeper zookeeper{"localhost:2181"};

	auto future = zookeeper.asyncGetChildren(argc <= 1 ? "/" : argv[1]);
	auto res = future.get();

	for (const auto & child : res)
		std::cerr << child << '\n';

	return 0;
}
catch (const Poco::Exception & e)
{
	std::cout << e.message() << std::endl;
	throw;
}
