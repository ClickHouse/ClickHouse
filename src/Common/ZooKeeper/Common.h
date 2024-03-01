#pragma once

#include "ZooKeeper.h"
#include <functional>

namespace zkutil
{

using GetZooKeeper = std::function<ZooKeeperPtr()>;

}
