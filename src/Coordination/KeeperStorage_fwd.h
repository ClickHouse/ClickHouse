#pragma once

#include "config.h"

namespace DB
{

struct KeeperMemNode;
struct KeeperRocksNode;

template<typename TContainer>
class KeeperStorage;

template <class V>
class SnapshotableHashTable;

template <class V>
struct RocksDBContainer;

using KeeperMemoryStorage = KeeperStorage<SnapshotableHashTable<KeeperMemNode>>;
#if USE_ROCKSDB
using KeeperRocksStorage = KeeperStorage<RocksDBContainer<KeeperRocksNode>>;
#endif

}
