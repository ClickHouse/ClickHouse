#include <Storages/TableZnodeInfo.h>

#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Databases/DatabaseReplicatedHelpers.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TableZnodeInfo TableZnodeInfo::resolve(const String & requested_path, const String & requested_replica_name, const StorageID & table_id, const ASTCreateQuery & query, LoadingStrictnessLevel mode, const ContextPtr & context)
{
    bool is_on_cluster = context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY;
    bool is_replicated_database = context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY &&
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->getEngineName() == "Replicated";

    /// Allow implicit {uuid} macros only for zookeeper_path in ON CLUSTER queries
    /// and if UUID was explicitly passed in CREATE TABLE (like for ATTACH)
    bool allow_uuid_macro = is_on_cluster || is_replicated_database || query.attach || query.has_uuid;

    TableZnodeInfo res;
    res.full_path = requested_path;
    res.replica_name = requested_replica_name;

    /// Unfold {database} and {table} macro on table creation, so table can be renamed.
    if (mode < LoadingStrictnessLevel::ATTACH)
    {
        Macros::MacroExpansionInfo info;
        /// NOTE: it's not recursive
        info.expand_special_macros_only = true;
        info.table_id = table_id;
        /// Avoid unfolding {uuid} macro on this step.
        /// We did unfold it in previous versions to make moving table from Atomic to Ordinary database work correctly,
        /// but now it's not allowed (and it was the only reason to unfold {uuid} macro).
        info.table_id.uuid = UUIDHelpers::Nil;
        res.full_path = context->getMacros()->expand(res.full_path, info);

        info.level = 0;
        res.replica_name = context->getMacros()->expand(res.replica_name, info);
    }

    res.full_path_for_metadata = res.full_path;
    res.replica_name_for_metadata = res.replica_name;

    /// Expand other macros (such as {shard} and {replica}). We do not expand them on previous step
    /// to make possible copying metadata files between replicas.
    Macros::MacroExpansionInfo info;
    info.table_id = table_id;
    if (is_replicated_database)
    {
        auto database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
        info.shard = getReplicatedDatabaseShardName(database);
        info.replica = getReplicatedDatabaseReplicaName(database);
    }
    if (!allow_uuid_macro)
        info.table_id.uuid = UUIDHelpers::Nil;
    res.full_path = context->getMacros()->expand(res.full_path, info);
    bool expanded_uuid_in_path = info.expanded_uuid;

    info.level = 0;
    info.table_id.uuid = UUIDHelpers::Nil;
    res.replica_name = context->getMacros()->expand(res.replica_name, info);

    /// We do not allow renaming table with these macros in metadata, because zookeeper_path will be broken after RENAME TABLE.
    /// NOTE: it may happen if table was created by older version of ClickHouse (< 20.10) and macros was not unfolded on table creation
    /// or if one of these macros is recursively expanded from some other macro.
    /// Also do not allow to move table from Atomic to Ordinary database if there's {uuid} macro
    if (info.expanded_database || info.expanded_table)
        res.renaming_restrictions = RenamingRestrictions::DO_NOT_ALLOW;
    else if (info.expanded_uuid)
        res.renaming_restrictions = RenamingRestrictions::ALLOW_PRESERVING_UUID;

    res.zookeeper_name = zkutil::extractZooKeeperName(res.full_path);
    res.path = zkutil::extractZooKeeperPath(res.full_path, /* check_starts_with_slash */ mode <= LoadingStrictnessLevel::CREATE, getLogger(table_id.getNameForLogs()));
    res.path_prefix_for_drop = res.path;

    if (expanded_uuid_in_path)
    {
        /// When dropping table with znode path "/foo/{uuid}/bar/baz", delete not only
        /// "/foo/{uuid}/bar/baz" but also "/foo/{uuid}/bar" and "/foo/{uuid}" if they became empty.
        ///
        /// (We find the uuid substring by searching instead of keeping track of it when expanding
        ///  the macro. So in principle we may find a uuid substring that wasn't expanded from a
        ///  macro. This should be ok because we're searching for the *last* occurrence, so we'll get
        ///  a prefix at least as long as the correct one, so we won't delete znodes outside the
        ///  {uuid} path component. This sounds sketchy, but propagating string indices through macro
        ///  expansion passes is sketchy too (error-prone and more complex), and on balance this seems
        ///  better.)
        String uuid_str = toString(table_id.uuid);
        size_t i = res.path.rfind(uuid_str);
        if (i == String::npos)
            /// Possible if the macro is in the "<auxiliary_zookeeper_name>:/" prefix, but we probably
            /// don't want to allow that.
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't find uuid in zookeeper path after expanding {{uuid}} macro: {} (uuid {})", res.path, uuid_str);
        i += uuid_str.size();
        /// In case the path is "/foo/pika{uuid}chu/bar" (or "/foo/{uuid}{replica}/bar").
        while (i < res.path.size() && res.path[i] != '/')
            i += 1;
        res.path_prefix_for_drop = res.path.substr(0, i);
    }

    return res;
}

void TableZnodeInfo::dropAncestorZnodesIfNeeded(const zkutil::ZooKeeperPtr & zookeeper) const
{
    chassert(path.starts_with(path_prefix_for_drop));
    if (path_prefix_for_drop.empty() || path_prefix_for_drop.size() == path.size())
        return;
    chassert(path[path_prefix_for_drop.size()] == '/');

    String path_to_remove = path;
    while (path_to_remove.size() > path_prefix_for_drop.size())
    {
        size_t i = path_to_remove.find_last_of('/');
        chassert(i != String::npos && i >= path_prefix_for_drop.size());
        path_to_remove = path_to_remove.substr(0, i);

        Coordination::Error rc = zookeeper->tryRemove(path_to_remove);
        if (rc != Coordination::Error::ZOK)
            /// Znode not empty or already removed by someone else.
            break;
    }
}

}
