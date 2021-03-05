#include "LibraryDictionarySource.h"

#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <ext/bit_cast.h>
#include <ext/range.h>
#include <ext/scope_guard.h>
#include <Common/StringUtils/StringUtils.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "LibraryDictionarySourceExternal.h"
#include "registerDictionaries.h"
#include <IO/WriteBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int FILE_DOESNT_EXIST;
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int PATH_ACCESS_DENIED;
}


LibraryDictionarySource::LibraryDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix_,
    Block & sample_block_,
    const Context & context_,
    bool check_config)
    : log(&Poco::Logger::get("LibraryDictionarySource"))
    , dict_struct{dict_struct_}
    , config_prefix{config_prefix_}
    , path{config.getString(config_prefix + ".path", "")}
    , dictionary_id(createDictID())
    , sample_block{sample_block_}
    , context(context_)
{

    if (check_config)
    {
        const String dictionaries_lib_path = context.getDictionariesLibPath();
        if (!startsWith(path, dictionaries_lib_path))
            throw Exception(ErrorCodes::PATH_ACCESS_DENIED, "LibraryDictionarySource: Library path {} is not inside {}", path, dictionaries_lib_path);
    }

    if (!Poco::File(path).exists())
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "LibraryDictionarySource: Can't load library {}: file doesn't exist", Poco::File(path).path());

    bridge_helper = std::make_shared<LibraryBridgeHelper>(context, dictionary_id);

    auto res = bridge_helper->initLibrary(path, getLibrarySettingsString(config, config_prefix + ".settings"));

    if (!res)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failed to create shared library from path: {}", path);

    description.init(sample_block);
}


LibraryDictionarySource::~LibraryDictionarySource()
{
}


LibraryDictionarySource::LibraryDictionarySource(const LibraryDictionarySource & other)
    : log(&Poco::Logger::get("LibraryDictionarySource"))
    , dict_struct{other.dict_struct}
    , config_prefix{other.config_prefix}
    , path{other.path}
    , dictionary_id{other.dictionary_id}
    , sample_block{other.sample_block}
    , context(other.context)
    , description{other.description}
{
    bridge_helper = std::make_shared<LibraryBridgeHelper>(context, dictionary_id);
}


String LibraryDictionarySource::getLibrarySettingsString(const Poco::Util::AbstractConfiguration & config, const std::string & config_root)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_root, config_keys);
    std::string res;

    for (const auto & key : config_keys)
    {
        std::string key_name = key;
        auto bracket_pos = key.find('[');

        if (bracket_pos != std::string::npos && bracket_pos > 0)
            key_name = key.substr(0, bracket_pos);

        if (!res.empty())
            res += ' ';

        res += key_name + ' ' + config.getString(config_root + "." + key);
    }

    return res;
}


String LibraryDictionarySource::getDictAttributesString()
{
    std::string res;
    for (const auto & attr : dict_struct.attributes)
    {
        if (!res.empty())
            res += ',';
        res += attr.name;
    }

    return res;
}


String LibraryDictionarySource::getDictIdsString(const std::vector<UInt64> & ids)
{
    std::string res;
    for (const auto & id : ids)
    {
        if (!res.empty())
            res += ',';
        res += std::to_string(id);
    }

    return res;
}


BlockInputStreamPtr LibraryDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll {}", toString());
    return bridge_helper->loadAll(getDictAttributesString(), description.sample_block);
}


BlockInputStreamPtr LibraryDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());
    return bridge_helper->loadIds(getDictAttributesString(), getDictIdsString(ids), description.sample_block);
}


BlockInputStreamPtr LibraryDictionarySource::loadKeys(const Columns &/* key_columns */, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());
    return {};
    //return bridge_helper->loadIds(getDictAttributesStrig(), getDictIdsString(), escription.sample_block);

    //auto holder = std::make_unique<ClickHouseLibrary::Row[]>(key_columns.size());
    //std::vector<std::unique_ptr<ClickHouseLibrary::Field[]>> column_data_holders;

    //for (size_t i = 0; i < key_columns.size(); ++i)
    //{
    //    auto cell_holder = std::make_unique<ClickHouseLibrary::Field[]>(requested_rows.size());

    //    for (size_t j = 0; j < requested_rows.size(); ++j)
    //    {
    //        auto data_ref = key_columns[i]->getDataAt(requested_rows[j]);
    //        cell_holder[j] = ClickHouseLibrary::Field{.data = static_cast<const void *>(data_ref.data), .size = data_ref.size};
    //    }

    //    holder[i] = ClickHouseLibrary::Row{.data = static_cast<ClickHouseLibrary::Field *>(cell_holder.get()), .size = requested_rows.size()};
    //    column_data_holders.push_back(std::move(cell_holder));
    //}

    //ClickHouseLibrary::Table request_cols{.data = static_cast<ClickHouseLibrary::Row *>(holder.get()), .size = key_columns.size()};

    //void * data_ptr = nullptr;

    ///// Get function pointer before dataNew call because library->get may throw.
    //auto func_load_keys = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&request_cols))>("ClickHouseDictionary_v3_loadKeys");

    //data_ptr = library->get<decltype(data_ptr) (*)(decltype(lib_data))>("ClickHouseDictionary_v3_dataNew")(lib_data);
    //auto * data = func_load_keys(data_ptr, &settings->strings, &request_cols);
    //auto block = dataToBlock(description.sample_block, data);

    //SCOPE_EXIT(library->get<void (*)(decltype(lib_data), decltype(data_ptr))>("ClickHouseDictionary_v3_dataDelete")(lib_data, data_ptr));
}


bool LibraryDictionarySource::isModified() const
{
    //if (auto func_is_modified = library->tryGet<bool (*)(decltype(lib_data), decltype(&settings->strings))>("ClickHouseDictionary_v3_isModified"))
    //    return func_is_modified(lib_data, &settings->strings);

    return true;
}


bool LibraryDictionarySource::supportsSelectiveLoad() const
{
    //if (auto func_supports_selective_load = library->tryGet<bool (*)(decltype(lib_data), decltype(&settings->strings))>("ClickHouseDictionary_v3_supportsSelectiveLoad"))
    //    return func_supports_selective_load(lib_data, &settings->strings);

    return true;
}


DictionarySourcePtr LibraryDictionarySource::clone() const
{
    return std::make_unique<LibraryDictionarySource>(*this);
}


std::string LibraryDictionarySource::toString() const
{
    return path;
}


void registerDictionarySourceLibrary(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context,
                                 const std::string & /* default_database */,
                                 bool check_config) -> DictionarySourcePtr
    {
        return std::make_unique<LibraryDictionarySource>(dict_struct, config, config_prefix + ".library", sample_block, context, check_config);
    };

    factory.registerSource("library", create_table_source);
}


}
