#pragma once

#include <Common/ZooKeeper/ZooKeeper.h>

#include <memory>
#include <optional>

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

/// The replica state of one cluster of a `Replicated` database on its way from Keeper: the request is sent by
/// `DatabaseReplicated::requestReplicasInfo` and awaited and parsed by `DatabaseReplicated::awaitReplicasInfo`,
/// so that a caller with many databases (`system.clusters`) can have a bounded window of their requests in flight.
/// An empty handle (nothing requested, or the request could not be sent) awaits to an empty `ReplicasInfo`.
class PendingReplicasInfo
{
public:
    size_t numKeeperRequests() const { return responses ? responses->numKeeperRequests() : 0; }

private:
    friend class DatabaseReplicated;

    ClusterPtr cluster;
    std::optional<zkutil::ZooKeeper::MultiTryGetResponse> responses;
};

}
