#pragma once
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/RaftServerConfig.h>

namespace DB
{
ClusterUpdateActions joiningToClusterUpdates(const ClusterConfigPtr & cfg, std::string_view joining);
ClusterUpdateActions leavingToClusterUpdates(const ClusterConfigPtr & cfg, std::string_view leaving);
String serializeClusterConfig(const ClusterConfigPtr & cfg, const ClusterUpdateActions & updates = {});
}
