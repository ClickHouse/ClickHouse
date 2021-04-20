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

namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int FILE_DOESNT_EXIST;
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int PATH_ACCESS_DENIED;
}


class CStringsHolder
{
public:
    using Container = std::vector<std::string>;
    explicit CStringsHolder(const Container & strings_pass)
    {
        strings_holder = strings_pass;
        strings.size = strings_holder.size();
        ptr_holder = std::make_unique<ClickHouseLibrary::CString[]>(strings.size);
        strings.data = ptr_holder.get();
        size_t i = 0;
        for (auto & str : strings_holder)
        {
            strings.data[i] = str.c_str();
            ++i;
        }
    }

    ClickHouseLibrary::CStrings strings; // will pass pointer to lib

private:
    std::unique_ptr<ClickHouseLibrary::CString[]> ptr_holder = nullptr;
    Container strings_holder;
};


namespace
{
    constexpr auto lib_config_settings = ".settings";


    CStringsHolder getLibSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_root)
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(config_root, config_keys);
        CStringsHolder::Container strings;
        for (const auto & key : config_keys)
        {
            std::string key_name = key;
            auto bracket_pos = key.find('[');
            if (bracket_pos != std::string::npos && bracket_pos > 0)
                key_name = key.substr(0, bracket_pos);
            strings.emplace_back(key_name);
            strings.emplace_back(config.getString(config_root + "." + key));
        }
        return CStringsHolder(strings);
    }


    Block dataToBlock(const Block & sample_block, const ClickHouseLibrary::RawClickHouseLibraryTable data)
    {
        if (!data)
            throw Exception("LibraryDictionarySource: No data returned", ErrorCodes::EXTERNAL_LIBRARY_ERROR);

        const auto * columns_received = static_cast<const ClickHouseLibrary::Table *>(data);
        if (columns_received->error_code)
            throw Exception(
                "LibraryDictionarySource: Returned error: " + std::to_string(columns_received->error_code) + " "
                    + (columns_received->error_string ? columns_received->error_string : ""),
                ErrorCodes::EXTERNAL_LIBRARY_ERROR);

        MutableColumns columns = sample_block.cloneEmptyColumns();

        for (size_t col_n = 0; col_n < columns_received->size; ++col_n)
        {
            if (columns.size() != columns_received->data[col_n].size)
                throw Exception(
                    "LibraryDictionarySource: Returned unexpected number of columns: " + std::to_string(columns_received->data[col_n].size)
                        + ", must be " + std::to_string(columns.size()),
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

            for (size_t row_n = 0; row_n < columns_received->data[col_n].size; ++row_n)
            {
                const auto & field = columns_received->data[col_n].data[row_n];
                if (!field.data)
                {
                    /// sample_block contains null_value (from config) inside corresponding column
                    const auto & col = sample_block.getByPosition(row_n);
                    columns[row_n]->insertFrom(*(col.column), 0);
                }
                else
                {
                    const auto & size = field.size;
                    columns[row_n]->insertData(static_cast<const char *>(field.data), size);
                }
            }
        }

        return sample_block.cloneWithColumns(std::move(columns));
    }
}


LibraryDictionarySource::LibraryDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix_,
    Block & sample_block_,
    const Context & context,
    bool check_config)
    : log(&Poco::Logger::get("LibraryDictionarySource"))
    , dict_struct{dict_struct_}
    , config_prefix{config_prefix_}
    , path{config.getString(config_prefix + ".path", "")}
    , sample_block{sample_block_}
{

    if (check_config)
    {
        const String dictionaries_lib_path = context.getDictionariesLibPath();
        if (!startsWith(path, dictionaries_lib_path))
            throw Exception("LibraryDictionarySource: Library path " + path + " is not inside " + dictionaries_lib_path, ErrorCodes::PATH_ACCESS_DENIED);
    }

    if (!Poco::File(path).exists())
        throw Exception(
            "LibraryDictionarySource: Can't load library " + Poco::File(path).path() + ": file doesn't exist",
            ErrorCodes::FILE_DOESNT_EXIST);

    description.init(sample_block);
    library = std::make_shared<SharedLibrary>(path, RTLD_LAZY
#if defined(RTLD_DEEPBIND) && !defined(ADDRESS_SANITIZER) // Does not exists in FreeBSD. Cannot work with Address Sanitizer.
        | RTLD_DEEPBIND
#endif
    );
    settings = std::make_shared<CStringsHolder>(getLibSettings(config, config_prefix + lib_config_settings));

    if (auto lib_new = library->tryGet<ClickHouseLibrary::LibraryNewFunc>(ClickHouseLibrary::LIBRARY_CREATE_NEW_FUNC_NAME))
        lib_data = lib_new(&settings->strings, ClickHouseLibrary::log);
}

LibraryDictionarySource::LibraryDictionarySource(const LibraryDictionarySource & other)
    : log(&Poco::Logger::get("LibraryDictionarySource"))
    , dict_struct{other.dict_struct}
    , config_prefix{other.config_prefix}
    , path{other.path}
    , sample_block{other.sample_block}
    , library{other.library}
    , description{other.description}
    , settings{other.settings}
{
    if (auto lib_clone = library->tryGet<ClickHouseLibrary::LibraryCloneFunc>(ClickHouseLibrary::LIBRARY_CLONE_FUNC_NAME))
        lib_data = lib_clone(other.lib_data);
    else if (auto lib_new = library->tryGet<ClickHouseLibrary::LibraryNewFunc>(ClickHouseLibrary::LIBRARY_CREATE_NEW_FUNC_NAME))
        lib_data = lib_new(&settings->strings, ClickHouseLibrary::log);
}

LibraryDictionarySource::~LibraryDictionarySource()
{
    if (auto lib_delete = library->tryGet<ClickHouseLibrary::LibraryDeleteFunc>(ClickHouseLibrary::LIBRARY_DELETE_FUNC_NAME))
        lib_delete(lib_data);
}

BlockInputStreamPtr LibraryDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll {}", toString());

    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(dict_struct.attributes.size());
    ClickHouseLibrary::CStrings columns{static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()),
                                        dict_struct.attributes.size()};
    size_t i = 0;
    for (const auto & a : dict_struct.attributes)
    {
        columns.data[i] = a.name.c_str();
        ++i;
    }

    auto load_all_func = library->get<ClickHouseLibrary::LibraryLoadAllFunc>(ClickHouseLibrary::LIBRARY_LOAD_ALL_FUNC_NAME);
    auto data_new_func = library->get<ClickHouseLibrary::LibraryDataNewFunc>(ClickHouseLibrary::LIBRARY_DATA_NEW_FUNC_NAME);
    auto data_delete_func = library->get<ClickHouseLibrary::LibraryDataDeleteFunc>(ClickHouseLibrary::LIBRARY_DATA_DELETE_FUNC_NAME);

    ClickHouseLibrary::LibraryData data_ptr = data_new_func(lib_data);
    SCOPE_EXIT(data_delete_func(lib_data, data_ptr));

    ClickHouseLibrary::RawClickHouseLibraryTable data = load_all_func(data_ptr, &settings->strings, &columns);
    auto block = dataToBlock(description.sample_block, data);

    return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibraryDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    const ClickHouseLibrary::VectorUInt64 ids_data{ext::bit_cast<decltype(ClickHouseLibrary::VectorUInt64::data)>(ids.data()), ids.size()};
    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(dict_struct.attributes.size());
    ClickHouseLibrary::CStrings columns_pass{static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()),
                                             dict_struct.attributes.size()};
    size_t i = 0;
    for (const auto & a : dict_struct.attributes)
    {
        columns_pass.data[i] = a.name.c_str();
        ++i;
    }

    auto load_ids_func = library->get<ClickHouseLibrary::LibraryLoadIdsFunc>(ClickHouseLibrary::LIBRARY_LOAD_IDS_FUNC_NAME);
    auto data_new_func = library->get<ClickHouseLibrary::LibraryDataNewFunc>(ClickHouseLibrary::LIBRARY_DATA_NEW_FUNC_NAME);
    auto data_delete_func = library->get<ClickHouseLibrary::LibraryDataDeleteFunc>(ClickHouseLibrary::LIBRARY_DATA_DELETE_FUNC_NAME);

    ClickHouseLibrary::LibraryData data_ptr = data_new_func(lib_data);
    SCOPE_EXIT(data_delete_func(lib_data, data_ptr));

    ClickHouseLibrary::RawClickHouseLibraryTable data = load_ids_func(data_ptr, &settings->strings, &columns_pass, &ids_data);
    auto block = dataToBlock(description.sample_block, data);

    return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibraryDictionarySource::loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto holder = std::make_unique<ClickHouseLibrary::Row[]>(key_columns.size());
    std::vector<std::unique_ptr<ClickHouseLibrary::Field[]>> column_data_holders;
    for (size_t i = 0; i < key_columns.size(); ++i)
    {
        auto cell_holder = std::make_unique<ClickHouseLibrary::Field[]>(requested_rows.size());
        for (size_t j = 0; j < requested_rows.size(); ++j)
        {
            auto data_ref = key_columns[i]->getDataAt(requested_rows[j]);
            cell_holder[j] = ClickHouseLibrary::Field{.data = static_cast<const void *>(data_ref.data), .size = data_ref.size};
        }
        holder[i]
            = ClickHouseLibrary::Row{.data = static_cast<ClickHouseLibrary::Field *>(cell_holder.get()), .size = requested_rows.size()};

        column_data_holders.push_back(std::move(cell_holder));
    }

    ClickHouseLibrary::Table request_cols{.data = static_cast<ClickHouseLibrary::Row *>(holder.get()), .size = key_columns.size()};

    auto load_keys_func = library->get<ClickHouseLibrary::LibraryLoadKeysFunc>(ClickHouseLibrary::LIBRARY_LOAD_KEYS_FUNC_NAME);
    auto data_new_func = library->get<ClickHouseLibrary::LibraryDataNewFunc>(ClickHouseLibrary::LIBRARY_DATA_NEW_FUNC_NAME);
    auto data_delete_func = library->get<ClickHouseLibrary::LibraryDataDeleteFunc>(ClickHouseLibrary::LIBRARY_DATA_DELETE_FUNC_NAME);

    ClickHouseLibrary::LibraryData data_ptr = data_new_func(lib_data);
    SCOPE_EXIT(data_delete_func(lib_data, data_ptr));

    ClickHouseLibrary::RawClickHouseLibraryTable data = load_keys_func(data_ptr, &settings->strings, &request_cols);
    auto block = dataToBlock(description.sample_block, data);

    return std::make_shared<OneBlockInputStream>(block);
}

bool LibraryDictionarySource::isModified() const
{
    if (auto func_is_modified = library->tryGet<ClickHouseLibrary::LibraryIsModifiedFunc>(
        ClickHouseLibrary::LIBRARY_IS_MODIFIED_FUNC_NAME))
        return func_is_modified(lib_data, &settings->strings);

    return true;
}

bool LibraryDictionarySource::supportsSelectiveLoad() const
{
    if (auto func_supports_selective_load = library->tryGet<ClickHouseLibrary::LibrarySupportsSelectiveLoadFunc>(
            ClickHouseLibrary::LIBRARY_SUPPORTS_SELECTIVE_LOAD_FUNC_NAME))
        return func_supports_selective_load(lib_data, &settings->strings);

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
