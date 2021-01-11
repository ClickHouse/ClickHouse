#pragma once

#include <common/types.h>

#include <map>
#include <memory>

namespace DB
{

class IStorage;

using ConstStoragePtr = std::shared_ptr<const IStorage>;
using StoragePtr = std::shared_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;

}
