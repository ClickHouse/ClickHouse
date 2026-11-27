#pragma once
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/RaftServerConfig.h>

namespace DB
{
/// Not only adding/removing servers, but also changing their priorities.
ClusterUpdateActions joiningToClusterUpdates(const ClusterConfigPtr & cfg, std::string_view joining);

ClusterUpdateActions leavingToClusterUpdates(const ClusterConfigPtr & cfg, std::string_view leaving);

/// False if `updates` both change and remove a server, or leave the cluster without a participant.
bool isValidClusterUpdate(const ClusterConfigPtr & cfg, const ClusterUpdateActions & updates);

String serializeClusterConfig(const ClusterConfigPtr & cfg, const ClusterUpdateActions & updates = {});
}
