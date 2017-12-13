#include <Common/ZooKeeper/ZooKeeper.h>
#include <iostream>
#include <unistd.h>

using namespace zkutil;

int main()
{
    try
    {
        ZooKeeper zk("mtfilter01t:2181,metrika-test:2181,mtweb01t:2181", "", 5000);
        Strings children;

        std::cout << "create path" << std::endl;
        zk.create("/test", "old", zkutil::CreateMode::Persistent);
        zkutil::Stat stat;
        zkutil::EventPtr watch = std::make_shared<Poco::Event>();

        std::cout << "get path" << std::endl;
        zk.get("/test", &stat, watch);
        std::cout << "set path" << std::endl;
        zk.set("/test", "new");
        watch->wait();
        std::cout << "watch happened" << std::endl;
        std::cout << "remove path" << std::endl;
        zk.remove("/test");

        Ops ops;
        ops.emplace_back(std::make_unique<Op::Create>("/test", "multi1", zk.getDefaultACL(), CreateMode::Persistent));
        ops.emplace_back(std::make_unique<Op::SetData>("/test", "multi2", -1));
        ops.emplace_back(std::make_unique<Op::Remove>("/test", -1));
        std::cout << "multi" << std::endl;
        OpResultsPtr res = zk.multi(ops);
        std::cout << "path created: " << dynamic_cast<Op::Create &>(*ops[0]).getPathCreated() << std::endl;
    }
    catch (KeeperException & e)
    {
        std::cerr << "KeeperException " << e.what() << " " << e.message() << std::endl;
    }
    return 0;
}
