#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>
#include <functional>

namespace zkutil
{

using GetZooKeeper = std::function<ZooKeeperPtr()>;

}
