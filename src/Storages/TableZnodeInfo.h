#pragma once

#include <base/types.h>
#include <Storages/RenamingRestrictions.h>
#include <Databases/LoadingStrictnessLevel.h>

namespace zkutil
{
class ZooKeeper;
using ZooKeeperPtr = std::shared_ptr<ZooKeeper>;
}

namespace DB
{

struct StorageID;
class ASTCreateQuery;
class Context;
using ContextPtr = std::shared_ptr<const Context>;

/// Helper for replicated tables that use zookeeper for coordination among replicas.
/// Handles things like:
///  * Expanding macros like {table} and {uuid} in zookeeper path. Some macros are expanded+saved once
///    on table creation (e.g. {table}, to avoid changing the path if the table is later renamed),
///    others are expanded on each server startup and each replica (e.g. {replica} because it's
///    different on different replicas).
///  * When dropping table with znode path (say) "/clickhouse/tables/{uuid}/{shard}", delete not only
///    the znode at this path but also the parent znode "/clickhouse/tables/{uuid}" if it became empty.
///    Otherwise each created+dropped table would leave behind an empty znode.

struct TableZnodeInfo
{
    String path;
    String replica_name;
    /// Which zookeeper cluster to use ("default" or one of auxiliary zookeepers listed in config).
    String zookeeper_name = "default";

    /// Path with optional zookeeper_name prefix: "<auxiliary_zookeeper_name>:<path>".
    String full_path;

    /// Do not allow RENAME TABLE if zookeeper_path contains {database} or {table} macro.
    RenamingRestrictions renaming_restrictions = RenamingRestrictions::ALLOW_ANY;

    /// Information to save in table metadata and send to replicas (if ON CLUSTER or DatabaseReplicated).
    /// Has some macros expanded (e.g. {table}), others left unexpanded (e.g. {replica}).
    String full_path_for_metadata;
    String replica_name_for_metadata;

    /// Path to an ancestor of `path` that should be considered "owned" by this table (shared among
    /// replicas of the table). When table is dropped, this znode will be removed if it became empty.
    /// E.g. path = "/clickhouse/tables/{uuid}/{shard}", path_prefix_to_drop = "/clickhouse/tables/{uuid}".
    String path_prefix_for_drop;

    static TableZnodeInfo resolve(
        const String & requested_path, const String & requested_replica_name,
        const StorageID & table_id, const ASTCreateQuery & query, LoadingStrictnessLevel mode,
        const ContextPtr & context);

    void dropAncestorZnodesIfNeeded(const zkutil::ZooKeeperPtr & zookeeper) const;
};

}
