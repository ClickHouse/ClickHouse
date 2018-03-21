#pragma once
#include <common/Types.h>
#include <future>
#include <memory>
#include <vector>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Poco/Event.h>


namespace zkutil
{

using Stat = ZooKeeperImpl::ZooKeeper::Stat;
using Strings = std::vector<std::string>;


namespace CreateMode
{
    extern const int Persistent;
    extern const int Ephemeral;
    extern const int EphemeralSequential;
    extern const int PersistentSequential;
}

using EventPtr = std::shared_ptr<Poco::Event>;

/// Callback to call when the watch fires.
/// Because callbacks are called in the single "completion" thread internal to libzookeeper,
/// they must execute as quickly as possible (preferably just set some notification).
using WatchCallback = ZooKeeperImpl::ZooKeeper::WatchCallback;

using Requests = ZooKeeperImpl::ZooKeeper::Requests;
using Responses = ZooKeeperImpl::ZooKeeper::Responses;

}
