#pragma once

#include "ZooKeeperHolder.h"

namespace zkutil
{

class Increment
{
public:
    Increment(ZooKeeperHolderPtr zk_holder_, const std::string & path_)
    : zookeeper_holder(zk_holder_), path(path_)
    {
        zookeeper_holder->getZooKeeper()->createAncestors(path);
    }

    size_t get()
    {
        LOG_TRACE(log, "Get increment");

        size_t result = 0;
        std::string result_str;
        Coordination::Stat stat;

        bool success = false;
        auto zookeeper = zookeeper_holder->getZooKeeper();
        do
        {
            if (zookeeper->tryGet(path, result_str, &stat))
            {
                result = std::stol(result_str) + 1;
                success = zookeeper->trySet(path, std::to_string(result), stat.version) == Coordination::Error::ZOK;
            }
            else
            {
                success = zookeeper->tryCreate(path, std::to_string(result), zkutil::CreateMode::Persistent) == Coordination::Error::ZOK;
            }
        }
        while (!success);

        return result;
    }
private:
    zkutil::ZooKeeperHolderPtr zookeeper_holder;
    std::string path;
    Poco::Logger * log = &Poco::Logger::get("zkutil::Increment");
};

}
