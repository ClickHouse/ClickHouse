#include <boost/algorithm/string/join.hpp>
#include <Poco/Util/AbstractConfiguration.h>

#include <Columns/ColumnsNumber.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Common/JSONBuilder.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromString.h>

#include "StorageSystemConfigs.h"

namespace DB
{

StorageSystemConfigs::StorageSystemConfigs(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(ColumnsDescription{names_and_types_list});
    setInMemoryMetadata(storage_metadata);
}

/*
static void configToObject(const Poco::Util::AbstractConfiguration & config, const String & prefix, Object & obj)
{
    std::cout << "configToObject prefix:" << prefix << std::endl;
    std::vector<String> keys;
    config.keys(prefix, keys);

    if (keys.empty())
    {
        std::cout << "getString prefix:" << prefix << std::endl;
        obj.emplace(prefix, config.getString(prefix));
        return;
    }
    std::cout << "keys:" << boost::algorithm::join(keys, ",") << std::endl;

    std::unordered_map<String, std::vector<String>> keys_map;
    for (const auto & key : keys)
    {
        auto pos = key.find_first_of('[');
        auto obj_key = pos == std::string::npos ? key : key.substr(0, pos);
        if (obj_key == "system")
            continue;

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

static Object configToObject(const Poco::Util::AbstractConfiguration & config)
{
    Object obj;
    configToObject(config, "", obj);
    return obj;
}
*/

static JSONBuilder::ItemPtr configToJSON(
    const Poco::Util::AbstractConfiguration & config, const String & prefix = "")
{
    std::vector<String> keys;
    config.keys(prefix, keys);
    if (keys.empty())
        return std::make_unique<JSONBuilder::JSONString>(config.getString(prefix));

    std::unordered_map<String, std::vector<String>> keys_map;
    for (const auto & key : keys)
    {
        auto pos = key.find_first_of('[');
        auto obj_key = pos == std::string::npos ? key : key.substr(0, pos);
        if (obj_key == "system" || obj_key == "daemon" || obj_key == "applicatioin")
            continue;
        keys_map[obj_key].push_back(key);
    }
    
    auto map = std::make_unique<JSONBuilder::JSONMap>();
    for (const auto & [obj_key, keys_list] : keys_map)
    {
        if (keys_list.size() == 1)
        {
            map->add(obj_key, configToJSON(config, prefix + (prefix.empty() ? "" : ".") + keys_list[0]));
        }
        else
        {
            auto array = std::make_unique<JSONBuilder::JSONArray>();
            for (const auto & key : keys_list)
            {
                array->add(configToJSON(config, prefix + (prefix.empty() ? "" : ".") + key));
            }
            map->add(obj_key, std::move(array));
        }
    }
    return std::move(map);
}

Pipe StorageSystemConfigs::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    storage_snapshot->check(column_names);

    Block header;
    for (const auto & name_and_type : names_and_types_list)
        header.insert({name_and_type.type->createColumn(), name_and_type.type, name_and_type.name});
    MutableColumns columns = header.cloneEmptyColumns();

    WriteBufferFromOwnString wbuf;
    auto json = configToJSON(context->getConfigRef());
    FormatSettings format_settings;
    JSONBuilder::FormatContext format_context{.out = wbuf};
    json->format({.settings = format_settings}, format_context);
    columns[0]->insert(wbuf.str());

    /*
    auto json_str = wbuf.str();
    std::replace(json_str.begin(), json_str.end(), '\n', ' ');
    std::replace(json_str.begin(), json_str.end(), '\t', ' ');

    std::cout << "json: " << json_str << std::endl;
    ReadBufferFromString rbuf(json_str);
    auto serialization = names_and_types_list.front().type->getDefaultSerialization();
    serialization->deserializeWholeText(*columns[0], rbuf, format_settings);
    */

    Chunk chunk({std::move(columns)}, 1);
    return Pipe(std::make_shared<SourceFromSingleChunk>(std::move(header), std::move(chunk)));
}


}
