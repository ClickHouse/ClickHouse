#pragma once

namespace DB
{

template<typename NodesStorage>
class KeeperStorageImpl;

struct KeeperMemNodesStorage;

using KeeperMemoryStorage = KeeperStorageImpl<KeeperMemNodesStorage>;

}
