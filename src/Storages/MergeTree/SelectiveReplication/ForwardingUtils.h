#pragma once

#include <Client/ConnectionPool.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/ReplicatedMergeTreeAddress.h>

namespace DB::SelectiveReplication::ForwardingUtils
{

/// Create a connection pool to a specific replica using its ZK-registered address.
/// Handles interserver credentials, compression, and TLS setup.
///
/// @param address   Target replica address (host, port, database) from ZK.
/// @param context   Server context used to obtain interserver credentials and scheme.
/// @param pool_size Maximum number of connections in the pool.
ConnectionPoolPtr createReplicaPool(
    const ReplicatedMergeTreeAddress & address,
    ContextPtr context,
    unsigned pool_size);

}
