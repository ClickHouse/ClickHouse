#include <iostream>
#include <Common/ZooKeeper/Lock.h>

int main()
{

    try
    {
        auto zookeeper_holder = std::make_shared<zkutil::ZooKeeperHolder>();
        zookeeper_holder->init("localhost:2181");

        zkutil::Lock l(zookeeper_holder, "/test", "test_lock");
        std::cout << "check " << l.tryCheck() << std::endl;
        std::cout << "lock tryLock() " << l.tryLock() << std::endl;
        std::cout << "check " << l.tryCheck() << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cout << e.message() << std::endl;
    }
    return 0;
}
