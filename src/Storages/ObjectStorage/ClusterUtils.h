#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorageTableOptions.h>
#include <Storages/ObjectStorage/IObjectIterator.h>

namespace DB
{

class Cluster;
using ClusterPtr = std::shared_ptr<Cluster>;

/// Shared implementation of updateQueryToSendIfNeeded for all object-storage
/// cluster storages (s3Cluster, icebergCluster, deltaLakeCluster, etc.).
/// Rewrites the table function arguments to include explicit schema and format
/// so that remote nodes do not have to re-infer them.
void updateClusterQueryToSendIfNeeded(
    ASTPtr & query,
    const StorageSnapshotPtr & storage_snapshot,
    const ContextPtr & context,
    const ObjectStorageConnectionConfigurationPtr & configuration,
    const StorageObjectStorageTableOptions & table_options,
    const String & engine_name);

/// Shared implementation of the iterator-to-Extension conversion used by
/// getTaskIteratorExtension in all object-storage cluster storages.
/// Takes an already-created file/object iterator and wraps it into a
/// RemoteQueryExecutor::Extension with bucket splitting and stable task distribution.
RemoteQueryExecutor::Extension buildClusterTaskIteratorExtension(
    std::shared_ptr<IObjectIterator> iterator,
    const StorageObjectStorageTableOptions & table_options,
    const ObjectStoragePtr & object_storage,
    const ContextPtr & local_context,
    ClusterPtr cluster);

}
