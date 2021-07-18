#include "LibraryDictionarySource.h"

#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>
#include <Common/filesystemHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <filesystem>

#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/registerDictionaries.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int PATH_ACCESS_DENIED;
}


LibraryDictionarySource::LibraryDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix_,
    Block & sample_block_,
    ContextPtr context_,
    bool created_from_ddl)
    : log(&Poco::Logger::get("LibraryDictionarySource"))
    , dict_struct{dict_struct_}
    , config_prefix{config_prefix_}
    , path{config.getString(config_prefix + ".path", "")}
    , dictionary_id(getDictID())
    , sample_block{sample_block_}
    , context(Context::createCopy(context_))
{
    if (created_from_ddl && !pathStartsWith(path, context->getDictionariesLibPath()))
        throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "File path {} is not inside {}", path, context->getDictionariesLibPath());

    if (!fs::exists(path))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "LibraryDictionarySource: Can't load library {}: file doesn't exist", path);

    description.init(sample_block);
    bridge_helper = std::make_shared<LibraryBridgeHelper>(context, description.sample_block, dictionary_id);
    auto res = bridge_helper->initLibrary(path, getLibrarySettingsString(config, config_prefix + ".settings"), getDictAttributesString());

    if (!res)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Failed to create shared library from path: {}", path);
}


LibraryDictionarySource::~LibraryDictionarySource()
{
    bridge_helper->removeLibrary();
}


LibraryDictionarySource::LibraryDictionarySource(const LibraryDictionarySource & other)
    : log(&Poco::Logger::get("LibraryDictionarySource"))
    , dict_struct{other.dict_struct}
    , config_prefix{other.config_prefix}
    , path{other.path}
    , dictionary_id{getDictID()}
    , sample_block{other.sample_block}
    , context(other.context)
    , description{other.description}
{
    bridge_helper = std::make_shared<LibraryBridgeHelper>(context, description.sample_block, dictionary_id);
    bridge_helper->cloneLibrary(other.dictionary_id);
}


bool LibraryDictionarySource::isModified() const
{
    return bridge_helper->isModified();
}


bool LibraryDictionarySource::supportsSelectiveLoad() const
{
    return bridge_helper->supportsSelectiveLoad();
}


BlockInputStreamPtr LibraryDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll {}", toString());
    return bridge_helper->loadAll();
}


BlockInputStreamPtr LibraryDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());
    return bridge_helper->loadIds(getDictIdsString(ids));
}


BlockInputStreamPtr LibraryDictionarySource::loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());
    auto block = blockForKeys(dict_struct, key_columns, requested_rows);
    return bridge_helper->loadKeys(block);
}


DictionarySourcePtr LibraryDictionarySource::clone() const
{
    return std::make_unique<LibraryDictionarySource>(*this);
}


std::string LibraryDictionarySource::toString() const
{
    return path;
}


String LibraryDictionarySource::getLibrarySettingsString(const Poco::Util::AbstractConfiguration & config, const std::string & config_root)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_root, config_keys);
    WriteBufferFromOwnString out;
    std::vector<std::string> settings;

    for (const auto & key : config_keys)
    {
        std::string key_name = key;
        auto bracket_pos = key.find('[');

        if (bracket_pos != std::string::npos && bracket_pos > 0)
            key_name = key.substr(0, bracket_pos);

        settings.push_back(key_name);
        settings.push_back(config.getString(config_root + "." + key));
    }

    writeVectorBinary(settings, out);
    return out.str();
}


String LibraryDictionarySource::getDictIdsString(const std::vector<UInt64> & ids)
{
    WriteBufferFromOwnString out;
    writeVectorBinary(ids, out);
    return out.str();
}


String LibraryDictionarySource::getDictAttributesString()
{
    std::vector<String> attributes_names(dict_struct.attributes.size());
    for (size_t i = 0; i < dict_struct.attributes.size(); ++i)
        attributes_names[i] = dict_struct.attributes[i].name;
    WriteBufferFromOwnString out;
    writeVectorBinary(attributes_names, out);
    return out.str();
}


void registerDictionarySourceLibrary(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        return std::make_unique<LibraryDictionarySource>(dict_struct, config, config_prefix + ".library", sample_block, context, created_from_ddl);
    };

    factory.registerSource("library", create_table_source);
}


}
