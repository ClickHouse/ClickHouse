#include <Common/Scheduler/Workload/WorkloadEntityConfigStorage.h>

#include <Parsers/ASTCreateWorkloadQuery.h>
#include <Parsers/ASTCreateResourceQuery.h>
#include <Parsers/ParserCreateWorkloadQuery.h>
#include <Parsers/ParserCreateResourceQuery.h>
#include <Parsers/parseQuery.h>
#include <Core/Settings.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

WorkloadEntityConfigStorage::WorkloadEntityConfigStorage(const ContextPtr & global_context_)
    : WorkloadEntityStorageBase(global_context_)
{
}

void WorkloadEntityConfigStorage::loadEntities()
{
    if (entities_loaded.exchange(true))
        return;
    
    // Config entities are loaded via updateConfiguration, not here
    // Just mark as loaded so subsequent calls don't do anything
}

void WorkloadEntityConfigStorage::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    current_config = &config;
    refreshEntities(config);
}

WorkloadEntityConfigStorage::OperationResult WorkloadEntityConfigStorage::storeEntityImpl(
    const ContextPtr &,
    WorkloadEntityType,
    const String &,
    ASTPtr,
    bool,
    bool,
    const Settings &)
{
    // Config storage is read-only - entities come from config, not SQL
    return OperationResult::Failed;
}

WorkloadEntityConfigStorage::OperationResult WorkloadEntityConfigStorage::removeEntityImpl(
    const ContextPtr &,
    WorkloadEntityType,
    const String &,
    bool)
{
    // Config storage is read-only - entities come from config, not SQL
    return OperationResult::Failed;
}

void WorkloadEntityConfigStorage::refreshEntities(const Poco::Util::AbstractConfiguration & config)
{
    LOG_DEBUG(log, "Refreshing workload entities from configuration");

    std::vector<std::pair<String, ASTPtr>> new_entities;

    // Load resources first (workloads may reference them)
    if (config.has("predefined_resources"))
    {
        Poco::Util::AbstractConfiguration::Keys resource_names;
        config.keys("predefined_resources", resource_names);

        for (const auto & resource_name : resource_names)
        {
            String config_key = "predefined_resources." + resource_name;
            if (!config.has(config_key + ".sql"))
            {
                LOG_WARNING(log, "Resource '{}' is missing 'sql' configuration", resource_name);
                continue;
            }

            String sql = config.getString(config_key + ".sql");
            
            if (auto ast = parseEntityFromConfig(WorkloadEntityType::Resource, resource_name, sql))
                new_entities.emplace_back(resource_name, ast);
        }
    }

    // Load workloads after resources
    if (config.has("predefined_workloads"))
    {
        Poco::Util::AbstractConfiguration::Keys workload_names;
        config.keys("predefined_workloads", workload_names);

        for (const auto & workload_name : workload_names)
        {
            String config_key = "predefined_workloads." + workload_name;
            if (!config.has(config_key + ".sql"))
            {
                LOG_WARNING(log, "Workload '{}' is missing 'sql' configuration", workload_name);
                continue;
            }

            String sql = config.getString(config_key + ".sql");
            
            if (auto ast = parseEntityFromConfig(WorkloadEntityType::Workload, workload_name, sql))
                new_entities.emplace_back(workload_name, ast);
        }
    }

    // Update entities in memory and notify subscribers
    setAllEntities(new_entities);

    LOG_DEBUG(log, "Loaded {} workload entities from configuration", new_entities.size());
}

ASTPtr WorkloadEntityConfigStorage::parseEntityFromConfig(WorkloadEntityType entity_type, const String & entity_name, const String & sql)
{
    try
    {
        ASTPtr ast;
        
        if (entity_type == WorkloadEntityType::Resource)
        {
            ParserCreateResourceQuery parser;
            const char * begin = sql.data();
            const char * end = sql.data() + sql.size();
            ast = parseQuery(parser, begin, end, "", 0, 0, 0);
            
            if (ast)
            {
                if (auto * create_resource = typeid_cast<ASTCreateResourceQuery *>(ast.get()))
                {
                    // Verify that the resource name in SQL matches the config key
                    if (create_resource->getResourceName() != entity_name)
                    {
                        LOG_WARNING(log, "Resource name mismatch in config: key '{}' vs SQL '{}'", 
                                   entity_name, create_resource->getResourceName());
                        return nullptr;
                    }
                }
                else
                {
                    LOG_WARNING(log, "Failed to parse resource '{}' from configuration: not a CREATE RESOURCE query", entity_name);
                    return nullptr;
                }
            }
        }
        else if (entity_type == WorkloadEntityType::Workload)
        {
            ParserCreateWorkloadQuery parser;
            const char * begin = sql.data();
            const char * end = sql.data() + sql.size();
            ast = parseQuery(parser, begin, end, "", 0, 0, 0);
            
            if (ast)
            {
                if (auto * create_workload = typeid_cast<ASTCreateWorkloadQuery *>(ast.get()))
                {
                    // Verify that the workload name in SQL matches the config key
                    if (create_workload->getWorkloadName() != entity_name)
                    {
                        LOG_WARNING(log, "Workload name mismatch in config: key '{}' vs SQL '{}'", 
                                   entity_name, create_workload->getWorkloadName());
                        return nullptr;
                    }
                }
                else
                {
                    LOG_WARNING(log, "Failed to parse workload '{}' from configuration: not a CREATE WORKLOAD query", entity_name);
                    return nullptr;
                }
            }
        }

        if (!ast)
        {
            LOG_WARNING(log, "Failed to parse {} '{}' from configuration: empty AST", 
                       entity_type == WorkloadEntityType::Resource ? "resource" : "workload", entity_name);
            return nullptr;
        }

        return ast;
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Failed to parse {} '{}' from configuration: {}", 
                   entity_type == WorkloadEntityType::Resource ? "resource" : "workload", entity_name, e.what());
        return nullptr;
    }
}

}