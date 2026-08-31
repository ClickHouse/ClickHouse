#pragma once

namespace DB
{

template<typename NodesStorage>
class KeeperStorageImpl;

struct KeeperMemNodesStorage;
struct KeeperLSMTNodesStorage;

using KeeperMemoryStorage = KeeperStorageImpl<KeeperMemNodesStorage>;
using KeeperLSMTStorage = KeeperStorageImpl<KeeperLSMTNodesStorage>;

}
