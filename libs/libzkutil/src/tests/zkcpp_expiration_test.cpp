#include <zkutil/ZooKeeper.h>
#include <iostream>
#include <unistd.h>

using namespace zkutil;
/** Проверяет, правда ли, что вызовы при просроченной сессии блокируются навсегда.
  * Разорвать сессию можно, например, так: `./nozk.sh && sleep 6s && ./yeszk.sh`
  */

void watcher(zhandle_t *zh, int type, int state, const char *path,void *watcherCtx)
{
}
int main()
{
	try
	{
		ZooKeeper zk("mtfilter01t:2181,metrika-test:2181,mtweb01t:2181", 5000);
		Strings children;
		children = zk.getChildren("/");
		for (auto s : children)
		{
			std::cout << s << std::endl;
		}
		sleep(5);

		children = zk.getChildren("/");
		for (auto s : children)
		{
			std::cout << s << std::endl;
		}

		Ops ops;
		std::string node = "/test";
		std::string value = "dummy";
		ops.push_back(new Op::Create(node, value, zk.getDefaultACL(), CreateMode::PersistentSequential));
		OpResultsPtr res = zk.multi(ops);
		std::cout << "path created: " << dynamic_cast<Op::Create &>(ops[0]).getPathCreated() << std::endl;
	}
	catch (KeeperException & e)
	{
		std::cerr << "KeeperException " << e.what() << " " << e.message() << std::endl;
	}
	return 0;
}
