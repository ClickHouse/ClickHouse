#include <DataStreams/OneBlockInputStream.h>
#include <Dictionaries/LibraryDictionarySource.h>
#include <Dictionaries/LibraryDictionarySourceExternal.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <ext/bit_cast.h>
#include <ext/range.h>
#include <ext/scope_guard.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int FILE_DOESNT_EXIST;
    extern const int EXTERNAL_LIBRARY_ERROR;
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


    Block dataToBlock(const Block & sample_block, const void * data)
    {
        if (!data)
            throw Exception("LibraryDictionarySource: No data returned", ErrorCodes::EXTERNAL_LIBRARY_ERROR);

        auto columns_received = static_cast<const ClickHouseLibrary::Table *>(data);
        if (columns_received->error_code)
            throw Exception("LibraryDictionarySource: Returned error: " + std::to_string(columns_received->error_code) + " "
                    + (columns_received->error_string ? columns_received->error_string : ""),
                ErrorCodes::EXTERNAL_LIBRARY_ERROR);

        MutableColumns columns(sample_block.columns());
        for (const auto i : ext::range(0, columns.size()))
            columns[i] = sample_block.getByPosition(i).column->cloneEmpty();

        for (size_t col_n = 0; col_n < columns_received->size; ++col_n)
        {
            if (columns.size() != columns_received->data[col_n].size)
                throw Exception("LibraryDictionarySource: Returned unexpected number of columns: "
                        + std::to_string(columns_received->data[col_n].size) + ", must be " + std::to_string(columns.size()),
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

            for (size_t row_n = 0; row_n < columns_received->data[col_n].size; ++row_n)
            {
                const auto & field = columns_received->data[col_n].data[row_n];
                if (!field.data)
                    continue;
                const auto & size = field.size;
                columns[row_n]->insertData(static_cast<const char *>(field.data), size);
            }
        }

        return sample_block.cloneWithColumns(std::move(columns));
    }
}


LibraryDictionarySource::LibraryDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Block & sample_block,
    const Context & context)
    : log(&Logger::get("LibraryDictionarySource"))
    , dict_struct{dict_struct_}
    , config_prefix{config_prefix}
    , path{config.getString(config_prefix + ".path", "")}
    , sample_block{sample_block}
    , context(context)
{
    if (!Poco::File(path).exists())
        throw Exception("LibraryDictionarySource: Can't load lib " + toString() + ": " + Poco::File(path).path() + " - File doesn't exist",
            ErrorCodes::FILE_DOESNT_EXIST);
    description.init(sample_block);
    library = std::make_shared<SharedLibrary>(path);
    settings = std::make_shared<CStringsHolder>(getLibSettings(config, config_prefix + lib_config_settings));
    if (auto libNew = library->tryGet<decltype(lib_data) (*)(decltype(&settings->strings), decltype(&ClickHouseLibrary::log))>(
            "ClickHouseDictionary_v3_libNew"))
        lib_data = libNew(&settings->strings, ClickHouseLibrary::log);
}

LibraryDictionarySource::LibraryDictionarySource(const LibraryDictionarySource & other)
    : log(&Logger::get("LibraryDictionarySource"))
    , dict_struct{other.dict_struct}
    , config_prefix{other.config_prefix}
    , path{other.path}
    , sample_block{other.sample_block}
    , context(other.context)
    , library{other.library}
    , description{other.description}
    , settings{other.settings}
{
    if (auto libClone = library->tryGet<decltype(lib_data) (*)(decltype(other.lib_data))>("ClickHouseDictionary_v3_libClone"))
        lib_data = libClone(other.lib_data);
    else if (auto libNew = library->tryGet<decltype(lib_data) (*)(decltype(&settings->strings), decltype(&ClickHouseLibrary::log))>(
                 "ClickHouseDictionary_v3_libNew"))
        lib_data = libNew(&settings->strings, ClickHouseLibrary::log);
}

LibraryDictionarySource::~LibraryDictionarySource()
{
    if (auto libDelete = library->tryGet<void (*)(decltype(lib_data))>("ClickHouseDictionary_v3_libDelete"))
        libDelete(lib_data);
}

BlockInputStreamPtr LibraryDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());

    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(dict_struct.attributes.size());
    ClickHouseLibrary::CStrings columns{
        static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()), dict_struct.attributes.size()};
    size_t i = 0;
    for (auto & a : dict_struct.attributes)
    {
        columns.data[i] = a.name.c_str();
        ++i;
    }
    void * data_ptr = nullptr;

    /// Get function pointer before dataNew call because library->get may throw.
    auto func_loadAll
        = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns))>("ClickHouseDictionary_v3_loadAll");
    data_ptr = library->get<decltype(data_ptr) (*)(decltype(lib_data))>("ClickHouseDictionary_v3_dataNew")(lib_data);
    auto data = func_loadAll(data_ptr, &settings->strings, &columns);
    auto block = dataToBlock(description.sample_block, data);
    SCOPE_EXIT(library->get<void (*)(decltype(lib_data), decltype(data_ptr))>("ClickHouseDictionary_v3_dataDelete")(lib_data, data_ptr));
    return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibraryDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());

    const ClickHouseLibrary::VectorUInt64 ids_data{ext::bit_cast<decltype(ClickHouseLibrary::VectorUInt64::data)>(ids.data()), ids.size()};
    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(dict_struct.attributes.size());
    ClickHouseLibrary::CStrings columns_pass{
        static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()), dict_struct.attributes.size()};
    size_t i = 0;
    for (auto & a : dict_struct.attributes)
    {
        columns_pass.data[i] = a.name.c_str();
        ++i;
    }
    void * data_ptr = nullptr;

    /// Get function pointer before dataNew call because library->get may throw.
    auto func_loadIds
        = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns_pass), decltype(&ids_data))>(
            "ClickHouseDictionary_v3_loadIds");
    data_ptr = library->get<decltype(data_ptr) (*)(decltype(lib_data))>("ClickHouseDictionary_v3_dataNew")(lib_data);
    auto data = func_loadIds(data_ptr, &settings->strings, &columns_pass, &ids_data);
    auto block = dataToBlock(description.sample_block, data);
    SCOPE_EXIT(library->get<void (*)(decltype(lib_data), decltype(data_ptr))>("ClickHouseDictionary_v3_dataDelete")(lib_data, data_ptr));
    return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibraryDictionarySource::loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());

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

    void * data_ptr = nullptr;
    /// Get function pointer before dataNew call because library->get may throw.
    auto func_loadKeys = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&request_cols))>(
        "ClickHouseDictionary_v3_loadKeys");
    data_ptr = library->get<decltype(data_ptr) (*)(decltype(lib_data))>("ClickHouseDictionary_v3_dataNew")(lib_data);
    auto data = func_loadKeys(data_ptr, &settings->strings, &request_cols);
    auto block = dataToBlock(description.sample_block, data);
    SCOPE_EXIT(library->get<void (*)(decltype(lib_data), decltype(data_ptr))>("ClickHouseDictionary_v3_dataDelete")(lib_data, data_ptr));
    return std::make_shared<OneBlockInputStream>(block);
}

bool LibraryDictionarySource::isModified() const
{
    if (auto func_isModified
        = library->tryGet<bool (*)(decltype(lib_data), decltype(&settings->strings))>("ClickHouseDictionary_v3_isModified"))
        return func_isModified(lib_data, &settings->strings);
    return true;
}

bool LibraryDictionarySource::supportsSelectiveLoad() const
{
    if (auto func_supportsSelectiveLoad
        = library->tryGet<bool (*)(decltype(lib_data), decltype(&settings->strings))>("ClickHouseDictionary_v3_supportsSelectiveLoad"))
        return func_supportsSelectiveLoad(lib_data, &settings->strings);
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
}
