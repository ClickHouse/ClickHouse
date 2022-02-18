#pragma once
#include <memory>

namespace DB
{

class IFileCache;
using FileCachePtr = std::shared_ptr<IFileCache>;

}
