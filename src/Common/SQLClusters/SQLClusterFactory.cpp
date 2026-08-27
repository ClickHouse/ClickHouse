#include <Common/SQLClusters/SQLClusterFactory.h>

#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Poco/Util/MapConfiguration.h>
#include <base/sleep.h>

#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int CLUSTER_ALREADY_EXISTS;
    extern const int CLUSTER_DOESNT_EXIST;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

namespace
{

using Properties = SettingsChanges;

bool hasProperty(const Properties & properties, const String & name)
{
    return std::any_of(properties.begin(), properties.end(), [&](const SettingChange & change) { return change.name == name; });
}

Properties mergeProperties(const Properties & base, const Properties & override_properties)
{
    Properties result = base;
    for (const auto & change : override_properties)
    {
        auto it = std::find_if(result.begin(), result.end(), [&](const SettingChange & existing) { return existing.name == change.name; });
        if (it != result.end())
            it->value = change.value;
        else
            result.push_back(change);
    }
    return result;
}

void setConfigValue(Poco::Util::MapConfiguration & config, const String & key, const Field & value)
{
    if (value.getType() == Field::Types::String)
        config.setString(key, value.safeGet<String>());
    else if (value.getType() == Field::Types::Bool)
        config.setBool(key, value.safeGet<bool>());
    else
        config.setUInt64(key, applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value));
}

void applyReplicaProperties(Poco::Util::MapConfiguration & config, const String & prefix, const Properties & properties)
{
    static const std::unordered_set<String> replica_keys = {
        "host", "port", "user", "password", "secure", "compression", "priority", "bind_host", "default_database"};

    for (const auto & change : properties)
    {
        if (!replica_keys.contains(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown replica property `{}`", change.name);
        setConfigValue(config, prefix + "." + change.name, change.value);
    }
}

void applyShardOnlyProperties(Poco::Util::MapConfiguration & config, const String & prefix, const Properties & properties)
{
    for (const auto & change : properties)
    {
        if (change.name == "weight" || change.name == "internal_replication")
            setConfigValue(config, prefix + "." + change.name, change.value);
    }
}

void applyClusterProperties(Poco::Util::MapConfiguration & config, const String & prefix, const Properties & properties)
{
    for (const auto & change : properties)
    {
        if (change.name == "secret" || change.name == "allow_distributed_ddl_queries")
            setConfigValue(config, prefix + "." + change.name, change.value);
        else if (change.name != "user" && change.name != "password")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown cluster property `{}`", change.name);
    }
}

}

ClusterPtr SQLClusterFactory::materializeCluster(const ASTCreateSQLClusterQuery & query, ContextPtr context)
{
    const auto & definition = query.definition->as<const ASTSQLClusterDefinition &>();
    const auto & settings = context->getSettingsRef();

    Poco::AutoPtr<Poco::Util::MapConfiguration> config = new Poco::Util::MapConfiguration();
    const String cluster_prefix = "cluster." + query.cluster_name;

    applyClusterProperties(*config, cluster_prefix, definition.cluster_properties);

    size_t shard_num = 0;
    for (const auto & shard_ast : definition.shards)
    {
        ++shard_num;
        const auto & shard = shard_ast->as<const ASTSQLClusterShard &>();
        const auto shard_properties = mergeProperties(definition.cluster_properties, shard.properties);

        if (shard.replicas.empty())
        {
            if (!hasProperty(shard_properties, "host"))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard {} in SQL cluster `{}` must have `host` or at least one REPLICA", shard_num, query.cluster_name);

            const String node_prefix = cluster_prefix + ".node" + std::to_string(shard_num);
            applyShardOnlyProperties(*config, node_prefix, shard_properties);
            applyReplicaProperties(*config, node_prefix, shard_properties);
        }
        else
        {
            const String shard_prefix = cluster_prefix + ".shard" + std::to_string(shard_num);
            applyShardOnlyProperties(*config, shard_prefix, shard_properties);

            size_t replica_num = 0;
            for (const auto & replica_ast : shard.replicas)
            {
                ++replica_num;
                const auto replica_properties = mergeProperties(shard_properties, replica_ast->as<const ASTSQLClusterReplica &>().properties);
                applyReplicaProperties(*config, shard_prefix + ".replica" + std::to_string(replica_num), replica_properties);
            }
        }
    }

    if (shard_num == 0)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "SQL cluster `{}` must contain at least one shard", query.cluster_name);

    return std::make_shared<Cluster>(*config, settings, "cluster", query.cluster_name);
}

SQLClusterFactory & SQLClusterFactory::instance()
{
    static SQLClusterFactory factory;
    return factory;
}

void SQLClusterFactory::shutdown()
{
    shutdown_called = true;
    if (update_task)
        update_task->deactivate();
    std::lock_guard lock(mutex);
    metadata_storage.reset();
}

bool SQLClusterFactory::isEnabled() const
{
    std::lock_guard lock(mutex);
    return metadata_storage != nullptr;
}

void SQLClusterFactory::loadIfNot()
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);
}

void SQLClusterFactory::loadIfNot(std::lock_guard<std::mutex> & lock)
{
    if (loaded)
        return;

    auto context = Context::getGlobalContextInstance();
    metadata_storage = SQLClusterMetadataStorage::create(context);
    if (!metadata_storage)
    {
        loaded = true;
        return;
    }

    reloadFromStorage();

    update_task = context->getSchedulePool().createTask(StorageID::createEmpty(), "SQLClusterMetadataStorage", [this] { updateFunc(); });
    update_task->activate();
    update_task->schedule();

    loaded = true;
}

void SQLClusterFactory::reloadFromStorage()
{
    auto context = Context::getGlobalContextInstance();
    const auto cluster_names = metadata_storage->listClusterNames();
    std::unordered_set<String> new_keeper_cluster_names(cluster_names.begin(), cluster_names.end());

    for (const auto & cluster_name : keeper_cluster_names)
    {
        if (!new_keeper_cluster_names.contains(cluster_name))
            context->removeCluster(cluster_name);
    }

    keeper_cluster_names = new_keeper_cluster_names;

    for (const auto & cluster_name : cluster_names)
    {
        const auto create_query = metadata_storage->readCreateQuery(cluster_name);
        auto cluster = materializeCluster(create_query, context);
        context->setCluster(cluster_name, cluster);
    }
}

void SQLClusterFactory::createFromSQL(const ASTCreateSQLClusterQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (!metadata_storage)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SQL cluster DDL is disabled: configure `<cluster_metadata>` to enable it");

    if (metadata_storage->exists(query.cluster_name))
    {
        if (query.if_not_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "SQL cluster `{}` already exists", query.cluster_name);
    }

    auto create_statement = query.formatWithSecretsOneLine();
    metadata_storage->writeCreateQuery(query.cluster_name, create_statement, false);

    auto context = Context::getGlobalContextInstance();
    context->setCluster(query.cluster_name, materializeCluster(query, context));
    keeper_cluster_names.insert(query.cluster_name);
}

void SQLClusterFactory::alterFromSQL(const ASTAlterSQLClusterQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (!metadata_storage)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SQL cluster DDL is disabled: configure `<cluster_metadata>` to enable it");

    if (!metadata_storage->exists(query.cluster_name))
    {
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "SQL cluster `{}` does not exist", query.cluster_name);
    }

    ASTCreateSQLClusterQuery create_query;
    create_query.cluster_name = query.cluster_name;
    create_query.definition = query.definition->clone();

    metadata_storage->writeCreateQuery(query.cluster_name, create_query.formatWithSecretsOneLine(), true);

    auto context = Context::getGlobalContextInstance();
    context->setCluster(query.cluster_name, materializeCluster(create_query, context));
}

void SQLClusterFactory::dropFromSQL(const ASTDropSQLClusterQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNot(lock);

    if (!metadata_storage)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SQL cluster DDL is disabled: configure `<cluster_metadata>` to enable it");

    if (!metadata_storage->removeIfExists(query.cluster_name))
    {
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "SQL cluster `{}` does not exist", query.cluster_name);
    }

    auto context = Context::getGlobalContextInstance();
    context->removeCluster(query.cluster_name);
    keeper_cluster_names.erase(query.cluster_name);
}

void SQLClusterFactory::updateFunc()
{
    LOG_TRACE(log, "SQL cluster metadata background updating thread started");

    while (!shutdown_called.load())
    {
        try
        {
            if (metadata_storage->waitUpdate())
            {
                std::lock_guard lock(mutex);
                if (metadata_storage)
                    reloadFromStorage();
            }
        }
        catch (const Coordination::Exception & e)
        {
            if (Coordination::isHardwareError(e.code))
            {
                LOG_INFO(log, "Lost ZooKeeper connection while syncing SQL clusters, will retry: {}", getCurrentExceptionMessage(true));
                sleepForSeconds(1);
            }
            else
            {
                tryLogCurrentException(log, __PRETTY_FUNCTION__);
                chassert(false);
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            chassert(false);
        }
    }

    LOG_TRACE(log, "SQL cluster metadata background updating thread finished");
}

}
