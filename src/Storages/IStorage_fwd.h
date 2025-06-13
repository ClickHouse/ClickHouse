#pragma once

#include <Core/Types_fwd.h>
#include <base/types.h>

#include <map>
#include <memory>

namespace DB
{

class IStorage;
struct SnapshotDetachedTable;

using ConstStoragePtr = std::shared_ptr<const IStorage>;
using StoragePtr = std::shared_ptr<IStorage>;
using Tables = std::map<String, StoragePtr>;
using SnapshotDetachedTables = std::map<String, SnapshotDetachedTable>;
}
