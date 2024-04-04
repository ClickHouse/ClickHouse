#include <Storages/System/StorageSystemGraphite.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemGraphite::getNamesAndTypes()
{
    return {
        {"config_name",     std::make_shared<DataTypeString>()},
        {"rule_type",       std::make_shared<DataTypeString>()},
        {"regexp",          std::make_shared<DataTypeString>()},
        {"function",        std::make_shared<DataTypeString>()},
        {"age",             std::make_shared<DataTypeUInt64>()},
        {"precision",       std::make_shared<DataTypeUInt64>()},
        {"priority",        std::make_shared<DataTypeUInt16>()},
        {"is_default",      std::make_shared<DataTypeUInt8>()},
        {"Tables.database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"Tables.table",    std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
    };
}


/*
 * Looking for (Replicated)*GraphiteMergeTree and get all configuration parameters for them
 */
static StorageSystemGraphite::Configs getConfigs(ContextPtr context)
{
    const Databases databases = DatabaseCatalog::instance().getDatabases();
    StorageSystemGraphite::Configs graphite_configs;

    for (const auto & db : databases)
    {
        /// Check if database can contain MergeTree tables
        if (!db.second->canContainMergeTreeTables())
            continue;

        for (auto iterator = db.second->getTablesIterator(context); iterator->isValid(); iterator->next())
        {
            const auto & table = iterator->table();
            if (!table)
                continue;

            const MergeTreeData * table_data = dynamic_cast<const MergeTreeData *>(table.get());
            if (!table_data)
                continue;

            if (table_data->merging_params.mode == MergeTreeData::MergingParams::Graphite)
            {
                const String & config_name = table_data->merging_params.graphite_params.config_name;

                auto table_id = table_data->getStorageID();
                if (!graphite_configs.contains(config_name))
                {
                    StorageSystemGraphite::Config new_config =
                    {
                        table_data->merging_params.graphite_params,
                        { table_id.database_name },
                        { table_id.table_name },
                    };
                    graphite_configs.emplace(config_name, new_config);
                }
                else
                {
                    graphite_configs[config_name].databases.emplace_back(table_id.database_name);
                    graphite_configs[config_name].tables.emplace_back(table_id.table_name);
                }
            }
        }
    }

    return graphite_configs;
}

void StorageSystemGraphite::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    Configs graphite_configs = getConfigs(context);

    for (const auto & config : graphite_configs)
    {
        UInt16 priority = 0;
        for (const auto & pattern : config.second.graphite_params.patterns)
        {
            bool is_default = pattern.regexp == nullptr;
            String regexp;
            String function;
            const String & rule_type = ruleTypeStr(pattern.rule_type);

            if (is_default)
            {
                priority = std::numeric_limits<UInt16>::max();
            }
            else
            {
                priority++;
                regexp = pattern.regexp_str;
            }

            if (pattern.function)
            {
                function = pattern.function->getName();
            }

            if (!pattern.retentions.empty())
            {
                for (const auto & retention : pattern.retentions)
                {
                    size_t i = 0;
                    res_columns[i++]->insert(config.first);
                    res_columns[i++]->insert(rule_type);
                    res_columns[i++]->insert(regexp);
                    res_columns[i++]->insert(function);
                    res_columns[i++]->insert(retention.age);
                    res_columns[i++]->insert(retention.precision);
                    res_columns[i++]->insert(priority);
                    res_columns[i++]->insert(is_default);
                    res_columns[i++]->insert(config.second.databases);
                    res_columns[i++]->insert(config.second.tables);
                }
            }
            else
            {
                size_t i = 0;
                res_columns[i++]->insert(config.first);
                res_columns[i++]->insert(rule_type);
                res_columns[i++]->insert(regexp);
                res_columns[i++]->insert(function);
                res_columns[i++]->insertDefault();
                res_columns[i++]->insertDefault();
                res_columns[i++]->insert(priority);
                res_columns[i++]->insert(is_default);
                res_columns[i++]->insert(config.second.databases);
                res_columns[i++]->insert(config.second.tables);
            }
        }
    }
}

}
