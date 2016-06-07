#pragma once

#include <zkutil/ZooKeeper.h>
#include <functional>

namespace zkutil
{

using GetZooKeeper = std::function<ZooKeeperPtr()>;

}
