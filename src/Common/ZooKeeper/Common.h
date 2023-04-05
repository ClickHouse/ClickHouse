#pragma once

#include <functional>

#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace zkutil
{

using GetZooKeeper = std::function<ZooKeeperPtr()>;
using GetZooKeeperWithFaultInjection = std::function<Coordination::ZooKeeperWithFaultInjection::Ptr()>;

}
