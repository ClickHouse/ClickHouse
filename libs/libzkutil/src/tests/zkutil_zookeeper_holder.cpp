#include <zkutil/ZooKeeperHolder.h>
#include <iostream>

int main()
{
	zkutil::ZooKeeperHolder::create("localhost:2181");

	{
		auto zk_handler = zkutil::ZooKeeperHolder::getInstance().getZooKeeper();
		bool started_new_session = zkutil::ZooKeeperHolder::getInstance().replaceZooKeeperSessionToNewOne();
		std::cerr << "Started new session: " << started_new_session << "\n";
		std::cerr << "get / " << zk_handler->get("/") << "\n";
	}

	{
		bool started_new_session = zkutil::ZooKeeperHolder::getInstance().replaceZooKeeperSessionToNewOne();
		std::cerr << "Started new session: " << started_new_session << "\n";
		auto zk_handler = zkutil::ZooKeeperHolder::getInstance().getZooKeeper();
		std::cerr << "get / " << zk_handler->get("/") << "\n";
	}

	return 0;
}