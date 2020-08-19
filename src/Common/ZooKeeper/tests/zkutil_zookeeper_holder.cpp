#include <Common/ZooKeeper/ZooKeeperHolder.h>
#include <iostream>

#include <Poco/Util/Application.h>

int main()
{
//    Test::initLogger();

    zkutil::ZooKeeperHolder zk_holder;
    zk_holder.init("localhost:2181");

    {
        auto zk_handler = zk_holder.getZooKeeper();
        if (zk_handler)
        {
            bool started_new_session = zk_holder.replaceZooKeeperSessionToNewOne();
            std::cerr << "Started new session: " << started_new_session << "\n";
            std::cerr << "get / " << zk_handler->get("/") << "\n";
        }
    }

    {
        bool started_new_session = zk_holder.replaceZooKeeperSessionToNewOne();
        std::cerr << "Started new session: " << started_new_session << "\n";
        auto zk_handler = zk_holder.getZooKeeper();
        if (zk_handler != nullptr)
            std::cerr << "get / " << zk_handler->get("/") << "\n";
    }

    return 0;
}
