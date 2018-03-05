#include <DataStreams/OneBlockInputStream.h>
#include <Dictionaries/LibraryDictionarySource.h>
#include <Dictionaries/LibraryDictionarySourceExternal.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <ext/bit_cast.h>
#include <ext/range.h>

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
    const std::string lib_config_settings = ".settings";


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
            return sample_block.cloneEmpty();

        auto columns_received = static_cast<const ClickHouseLibrary::Table *>(data);
        if (columns_received->error_code)
            throw Exception("Received error: " + std::to_string(columns_received->error_code) + " "
                    + (columns_received->error_string ? columns_received->error_string : ""),
                ErrorCodes::EXTERNAL_LIBRARY_ERROR);

        MutableColumns columns(sample_block.columns());
        for (const auto i : ext::range(0, columns.size()))
            columns[i] = sample_block.getByPosition(i).column->cloneEmpty();

        for (size_t col_n = 0; col_n < columns_received->size; ++col_n)
        {
            if (columns.size() != columns_received->data[col_n].size)
                throw Exception("Received unexpected number of columns: " + std::to_string(columns_received->data[col_n].size)
                        + ", must be " + std::to_string(columns.size()),
                    ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

            for (size_t row_n = 0; row_n < columns_received->data[col_n].size; ++row_n)
            {
                const auto & field = columns_received->data[col_n].data[row_n];
                if (!field.data)
                    continue;
                const auto & size = field.size;
                const auto & data = static_cast<const char *>(field.data);
                columns[row_n]->insertData(data, size);
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
    auto fptr = library->tryGet<void * (*)()>("ClickHouseDictionary_v2_libNew");
    if (fptr)
        lib_data = fptr();
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
    , lib_data{other.lib_data}
{
}

LibraryDictionarySource::~LibraryDictionarySource()
{
    auto fptr = library->tryGet<void * (*)(void *)>("ClickHouseDictionary_v2_libDelete");
    if (fptr)
        fptr(lib_data);
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
    auto fptr
        = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns))>("ClickHouseDictionary_v2_loadAll");
    data_ptr = library->get<void * (*)(void *)>("ClickHouseDictionary_v2_dataNew")(lib_data);
    auto data = fptr(data_ptr, &settings->strings, &columns);
    auto block = dataToBlock(description.sample_block, data);
    library->get<void (*)(void *, void *)>("ClickHouseDictionary_v2_dataDelete")(lib_data, data_ptr);
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
    auto fptr = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns_pass), decltype(&ids_data))>(
        "ClickHouseDictionary_v2_loadIds");
    data_ptr = library->get<void * (*)(void *)>("ClickHouseDictionary_v2_dataNew")(lib_data);
    auto data = fptr(data_ptr, &settings->strings, &columns_pass, &ids_data);
    auto block = dataToBlock(description.sample_block, data);
    library->get<void (*)(void *, void *)>("ClickHouseDictionary_v2_dataDelete")(lib_data, data_ptr);
    return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibraryDictionarySource::loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());

    auto columns_holder = std::make_unique<ClickHouseLibrary::CString[]>(key_columns.size());
    ClickHouseLibrary::CStrings columns_pass{
        static_cast<decltype(ClickHouseLibrary::CStrings::data)>(columns_holder.get()), key_columns.size()};
    size_t key_columns_n = 0;
    for (auto & column : key_columns)
    {
        columns_pass.data[key_columns_n] = column->getName().c_str();
        ++key_columns_n;
    }
    const ClickHouseLibrary::VectorUInt64 requested_rows_c{
        ext::bit_cast<decltype(ClickHouseLibrary::VectorUInt64::data)>(requested_rows.data()), requested_rows.size()};
    void * data_ptr = nullptr;

    /// Get function pointer before dataNew call because library->get may throw.
    auto fptr
        = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns_pass), decltype(&requested_rows_c))>(
            "ClickHouseDictionary_v2_loadKeys");
    data_ptr = library->get<void * (*)(void *)>("ClickHouseDictionary_v2_dataNew")(lib_data);
    auto data = fptr(data_ptr, &settings->strings, &columns_pass, &requested_rows_c);
    auto block = dataToBlock(description.sample_block, data);
    library->get<void (*)(void *, void *)>("ClickHouseDictionary_v2_dataDelete")(lib_data, data_ptr);
    return std::make_shared<OneBlockInputStream>(block);
}

bool LibraryDictionarySource::isModified() const
{
    auto fptr = library->tryGet<void * (*)(decltype(lib_data), decltype(&settings->strings))>("ClickHouseDictionary_v2_isModified");
    if (fptr)
        return fptr(lib_data, &settings->strings);
    return true;
}

bool LibraryDictionarySource::supportsSelectiveLoad() const
{
    auto fptr
        = library->tryGet<void * (*)(decltype(lib_data), decltype(&settings->strings))>("ClickHouseDictionary_v2_supportsSelectiveLoad");
    if (fptr)
        return fptr(lib_data, &settings->strings);
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
