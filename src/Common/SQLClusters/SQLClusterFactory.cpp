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
    extern const int BAD_ARGUMENTS;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

namespace
{

using Properties = SettingsChanges;

const std::unordered_set<String> & clusterPropertyKeys()
{
    static const std::unordered_set<String> keys = {"secret", "allow_distributed_ddl_queries"};
    return keys;
}

const std::unordered_set<String> & shardOnlyPropertyKeys()
{
    static const std::unordered_set<String> keys = {"weight", "internal_replication"};
    return keys;
}

const std::unordered_set<String> & replicaPropertyKeys()
{
    static const std::unordered_set<String> keys = {
        "host", "port", "user", "password", "secure", "compression", "priority", "bind_host", "default_database"};
    return keys;
}

bool isCredentialProperty(std::string_view name)
{
    return name == "user" || name == "password";
}

void validateClusterLevelProperties(const Properties & properties)
{
    for (const auto & change : properties)
    {
        if (!clusterPropertyKeys().contains(change.name) && !isCredentialProperty(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed at cluster level", change.name);
    }
}

void validateShardProperties(const Properties & properties, bool has_replicas)
{
    for (const auto & change : properties)
    {
        if (has_replicas)
        {
            if (!shardOnlyPropertyKeys().contains(change.name) && !isCredentialProperty(change.name))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed in SHARD with REPLICA blocks", change.name);
        }
        else if (!replicaPropertyKeys().contains(change.name) && !shardOnlyPropertyKeys().contains(change.name))
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed in SHARD", change.name);
        }
    }
}

void validateReplicaProperties(const Properties & properties)
{
    for (const auto & change : properties)
    {
        if (!replicaPropertyKeys().contains(change.name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Property `{}` is not allowed in REPLICA", change.name);
    }
}

void validateSQLClusterDefinition(const ASTSQLClusterDefinition & definition)
{
    validateClusterLevelProperties(definition.cluster_properties);

    for (const auto & shard_ast : definition.shards)
    {
        const auto & shard = shard_ast->as<const ASTSQLClusterShard &>();
        validateShardProperties(shard.properties, !shard.replicas.empty());

        for (const auto & replica_ast : shard.replicas)
            validateReplicaProperties(replica_ast->as<const ASTSQLClusterReplica &>().properties);
    }
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
    for (const auto & change : properties)
    {
        if (!replicaPropertyKeys().contains(change.name))
            continue;
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

ClusterPtr SQLClusterFactory::materializeCluster(
    const ASTCreateSQLClusterQuery & query,
    ContextPtr context,
    String create_statement)
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
        Properties shard_properties = definition.cluster_properties;
        shard_properties.setSettings(shard.properties);

        if (shard.replicas.empty())
        {
            if (!shard_properties.tryGet("host"))
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
                const auto & replica = replica_ast->as<const ASTSQLClusterReplica &>();
                Properties replica_properties = shard_properties;
                replica_properties.setSettings(replica.properties);
                applyReplicaProperties(*config, shard_prefix + ".replica" + std::to_string(replica_num), replica_properties);
            }
        }
    }

    if (shard_num == 0)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "SQL cluster `{}` must contain at least one shard", query.cluster_name);

    return std::make_shared<Cluster>(
        *config,
        settings,
        "cluster",
        query.cluster_name,
        Cluster::SourceId::SQL,
        std::move(create_statement));
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

void SQLClusterFactory::loadIfNot()
{
    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);
}

bool SQLClusterFactory::usesReplicatedStorage()
{
    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);
    return metadata_storage->isReplicated();
}

void SQLClusterFactory::loadIfNotImpl(std::lock_guard<std::mutex> &)
{
    if (loaded)
        return;

    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    metadata_storage = SQLClusterMetadataStorage::create(context);

    reloadFromStorage();

    if (metadata_storage->isReplicated())
    {
        update_task = context->getSchedulePool()->createTask(StorageID::createEmpty(), "SQLClusterMetadataStorage", [this] { updateFunc(); });
        update_task->activate();
        update_task->schedule();
    }

    loaded = true;
}

void SQLClusterFactory::reloadFromStorage()
{
    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    const auto cluster_names = metadata_storage->listClusterNames();
    std::unordered_set<String> new_stored_cluster_names(cluster_names.begin(), cluster_names.end());

    for (const auto & cluster_name : stored_cluster_names)
    {
        if (!new_stored_cluster_names.contains(cluster_name))
            context->removeCluster(cluster_name);
    }

    stored_cluster_names = new_stored_cluster_names;

    for (const auto & cluster_name : cluster_names)
    {
        const auto create_query = metadata_storage->readCreateQuery(cluster_name);
        auto create_statement = create_query.formatWithSecretsOneLine();
        auto cluster = materializeCluster(create_query, context, std::move(create_statement));
        context->setCluster(cluster_name, cluster);
    }
}

void SQLClusterFactory::createFromSQL(const ASTCreateSQLClusterQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);

    if (metadata_storage->exists(query.cluster_name))
    {
        if (query.if_not_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "SQL cluster `{}` already exists", query.cluster_name);
    }

    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    if (auto existing = context->tryGetCluster(query.cluster_name))
    {
        if (existing->getSourceId() == Cluster::SourceId::SQL)
            throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "SQL cluster `{}` already exists", query.cluster_name);

        if (existing->getSourceId() == Cluster::SourceId::CONFIG)
            throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Cluster `{}` already exists in server configuration", query.cluster_name);

        throw Exception(ErrorCodes::CLUSTER_ALREADY_EXISTS, "Cluster `{}` already exists (cluster discovery)", query.cluster_name);
    }

    validateSQLClusterDefinition(query.definition->as<const ASTSQLClusterDefinition &>());

    auto create_statement = query.formatWithSecretsOneLine();
    metadata_storage->writeCreateQuery(query.cluster_name, create_statement, false);

    context->setCluster(query.cluster_name, materializeCluster(query, context, create_statement));
    stored_cluster_names.insert(query.cluster_name);
}

void SQLClusterFactory::alterFromSQL(const ASTAlterSQLClusterQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);

    if (!metadata_storage->exists(query.cluster_name))
    {
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "SQL cluster `{}` does not exist", query.cluster_name);
    }

    ASTCreateSQLClusterQuery create_query;
    create_query.cluster_name = query.cluster_name;
    create_query.definition = query.definition->clone();

    validateSQLClusterDefinition(create_query.definition->as<const ASTSQLClusterDefinition &>());

    auto create_statement = create_query.formatWithSecretsOneLine();
    metadata_storage->writeCreateQuery(query.cluster_name, create_statement, true);

    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    context->setCluster(query.cluster_name, materializeCluster(create_query, context, create_statement));
}

void SQLClusterFactory::dropFromSQL(const ASTDropSQLClusterQuery & query)
{
    std::lock_guard lock(mutex);
    loadIfNotImpl(lock);

    if (!metadata_storage->removeIfExists(query.cluster_name))
    {
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "SQL cluster `{}` does not exist", query.cluster_name);
    }

    auto context = Context::getGlobalContextInstance()->getGlobalContext();
    context->removeCluster(query.cluster_name);
    stored_cluster_names.erase(query.cluster_name);
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
