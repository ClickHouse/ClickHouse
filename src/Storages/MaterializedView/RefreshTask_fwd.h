#pragma once

#include <base/types.h>
#include <memory>

namespace DB
{

class RefreshTask;

using RefreshTaskStateUnderlying = UInt8;
using RefreshTaskHolder = std::shared_ptr<RefreshTask>;
using RefreshTaskObserver = std::weak_ptr<RefreshTask>;

}
