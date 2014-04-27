#include <zookeeper/zookeeper.hh>

namespace zk = org::apache::zookeeper;

/** Проверяет, правда ли, что вызовы в zkcpp при просроченной сессии блокируются навсегда.
  * Разорвать сессию можно, например, так: `./nozk.sh && sleep 6s && ./yeszk.sh`
  */

void stateChanged(zk::WatchEvent::type event, zk::SessionState::type state, const std::string & path)
{
	std::cout << "state changed; event: " << zk::WatchEvent::toString(event) << ", state: " << zk::SessionState::toString(state)
		<< ", path: " << path << std::endl;
}

int main()
{
	zk::ZooKeeper zookeeper;
	zookeeper.init("example1:2181,example2:2181,example3:2181", 5000, nullptr);

	std::vector<std::string> children;
	zk::data::Stat stat;
	zk::ReturnCode::type ret = zookeeper.getChildren("/", nullptr, children, stat);

	std::cout << "getChildren returned " << zk::ReturnCode::toString(ret) << std::endl;
	std::cout << "children of /:" << std::endl;
	for (const auto & s : children)
	{
		std::cout << s << std::endl;
	}

	std::cout << "break connection to example1:2181,example2:2181,example3:2181 for at least 5 seconds and enter something" << std::endl;
	std::string unused;
	std::cin >> unused;

	children.clear();
	std::cout << "will getChildren (this call will block forever, which seems to be zkcpp issue)" << std::endl;
	ret = zookeeper.getChildren("/", nullptr, children, stat);

	std::cout << "getChildren returned " << zk::ReturnCode::toString(ret) << std::endl;
	std::cout << "children of /:" << std::endl;
	for (const auto & s : children)
	{
		std::cout << s << std::endl;
	}

	return 0;
}
