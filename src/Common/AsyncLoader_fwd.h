#pragma once

#include <vector>
#include <memory>
#include <unordered_set>

namespace DB
{

class AsyncLoader;
class LoadTask;
using LoadTaskPtr = std::shared_ptr<LoadTask>;
using LoadTaskPtrs = std::vector<LoadTaskPtr>;

class LoadJob;
using LoadJobPtr = std::shared_ptr<LoadJob>;
using LoadJobSet = std::unordered_set<LoadJobPtr>;

}
