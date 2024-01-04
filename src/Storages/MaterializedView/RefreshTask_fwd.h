#pragma once

#include <base/types.h>
#include <memory>

namespace DB
{

class RefreshTask;

using RefreshTaskHolder = std::shared_ptr<RefreshTask>;

}
