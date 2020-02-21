#pragma once

#include <Core/Types.h>

#include <map>
#include <memory>

namespace DB
{

class IStorage;

using StoragePtr = std::shared_ptr<IStorage>;
using StorageWeakPtr = std::weak_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;

}
