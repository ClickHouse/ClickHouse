#pragma once

#include <functional>

#include <Common/ZooKeeper/ZooKeeper.h>

namespace zkutil
{

using GetZooKeeper = std::function<ZooKeeperPtr()>;

}
