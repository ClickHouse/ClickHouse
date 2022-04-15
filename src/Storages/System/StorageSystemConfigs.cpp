#include <boost/algorithm/string/join.hpp>
#include <Poco/Util/AbstractConfiguration.h>

#include <Columns/ColumnsNumber.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Common/JSONBuilder.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>
#include <Access/Common/AccessFlags.h>

#include "StorageSystemConfigs.h"

namespace DB
{

StorageSystemConfigs::StorageSystemConfigs(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{names_and_types_list});
    setInMemoryMetadata(storage_metadata);
}

static JSONBuilder::ItemPtr configToJSON(
    const Poco::Util::AbstractConfiguration & config, const String & prefix = "")
{
    std::vector<String> keys;
    config.keys(prefix, keys);
    if (keys.empty())
        return std::make_unique<JSONBuilder::JSONString>(config.getString(prefix));

    std::unordered_map<String, std::vector<String>> key_mapping;
    for (const auto & key : keys)
    {
        auto pos = key.find_first_of('[');
        auto json_key = pos == std::string::npos ? key : key.substr(0, pos);

        /// Skip built-in configurations not in config.xml.
        if (json_key == "system" || json_key == "daemon" || json_key == "application")
            continue;
        key_mapping[json_key].push_back(key);
    }

    auto map = std::make_unique<JSONBuilder::JSONMap>();
    for (const auto & [json_key, keys_list] : key_mapping)
    {
        if (keys_list.size() == 1)
        {
            map->add(json_key, configToJSON(config, prefix + (prefix.empty() ? "" : ".") + keys_list[0]));
        }
        else
        {
            auto array = std::make_unique<JSONBuilder::JSONArray>();
            for (const auto & key : keys_list)
            {
                array->add(configToJSON(config, prefix + (prefix.empty() ? "" : ".") + key));
            }
            map->add(json_key, std::move(array));
        }
    }
    return std::move(map);
}

/*
static void configToObject(const Poco::Util::AbstractConfiguration & config, const String & prefix, Object & obj)
{
    std::vector<String> keys;
    config.keys(prefix, keys);
    if (keys.empty())
    {
        obj.emplace(prefix, config.getString(prefix));
        return;
    }

    std::unordered_map<String, std::vector<String>> keys_map;
    for (const auto & key : keys)
    {
        auto pos = key.find_first_of('[');
        auto obj_key = pos == std::string::npos ? key : key.substr(0, pos);
        keys_map[obj_key].push_back(key);
    }

    for (const auto & [obj_key, keys_list] : keys_map)
    {
        if (keys_list.size() == 1)
        {
            Object sub_obj;
            configToObject(config, prefix + (prefix.empty() ? "" : ".") + keys_list[0], sub_obj);
            obj.emplace(obj_key, std::move(sub_obj));
        }
        else
        {
            Array sub_arr;
            for (const auto & key : keys_list)
            {
                Object elem_obj;
                configToObject(config, prefix + (prefix.empty() ? "" : ".") + key, elem_obj);
                sub_arr.emplace_back(std::move(elem_obj));
            }
            obj.emplace(obj_key, std::move(sub_arr));
        }
    }
}

static std::map<String, Object> configToObjects(const Poco::Util::AbstractConfiguration & config)
{
    std::vector<String> keys;
    config.keys("", keys);
    if (keys.empty())
        return {};
    
    std::map<String, Object> res;
    for (const auto & key : keys)
    {
        if (key == "system" || key == "daemon" || key == "application")
            continue;

        Object child;
        configToObject(config, key, child);
        res.emplace(key, std::move(child));
    }
    return res;
}
*/

Pipe StorageSystemConfigs::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    /// Need access SYSTEM_CONFIGS to select system.configs.
    if (getStorageID().getFullNameNotQuoted() == "system.configs")
        context->checkAccess(AccessType::SYSTEM_CONFIGS, getStorageID());

    storage_snapshot->check(column_names);

    Block header;
    for (const auto & name_and_type : names_and_types_list)
        header.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});
    MutableColumns columns = header.cloneEmptyColumns();

    auto json = configToJSON(context->getConfigRef());
    const auto * json_map = static_cast<const JSONBuilder::JSONMap *>(json.get());
    FormatSettings format_settings;
    json_map->forEach(
        [&](const JSONBuilder::JSONMap::Pair & pair)
        {
            columns[0]->insert(pair.key);

            WriteBufferFromOwnString wbuf;
            JSONBuilder::FormatContext format_context{.out = wbuf};
            pair.value->format({.settings = format_settings}, format_context);
            columns[1]->insert(wbuf.str());
        });


    /*
    auto objs = configToObjects(context->getConfigRef());
    for (const auto & [name, config] : objs)
    {
        columns[0]->insert(name);
        columns[1]->insert(config);
    }
    */
    UInt64 num_rows = columns[0]->size();
    Chunk chunk({std::move(columns)}, num_rows);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}
}
