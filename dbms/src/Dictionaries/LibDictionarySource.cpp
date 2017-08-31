#include <DataStreams/OneBlockInputStream.h>
#include <Dictionaries/LibDictionarySource.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>
#include "LibDictionarySourceExternal.h"

#include <Common/iostream_debug_helpers.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int FILE_DOESNT_EXIST;
}

const std::string lib_config_settings = ".settings";

struct CStringsHolder
{
    ClickHouseLib::CStrings strings; // will pass pointer to lib
    std::unique_ptr<ClickHouseLib::CString[]> ptr_holder = nullptr;
    std::vector<std::string> stringHolder;

    void prepare()
    {
        strings.size = stringHolder.size();
        ptr_holder = std::make_unique<ClickHouseLib::CString[]>(strings.size);
        strings.data = ptr_holder.get();
        size_t i = 0;
        for (auto & str : stringHolder)
        {
            strings.data[i] = str.c_str();
            ++i;
        }
    }
};

CStringsHolder getLibSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_root)
{
    CStringsHolder holder;
    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_root, config_keys);
    for (const auto & key : config_keys)
    {
        std::string key_name = key;
        auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos && bracket_pos > 0)
            key_name = key.substr(0, bracket_pos);
        holder.stringHolder.emplace_back(key_name);
        holder.stringHolder.emplace_back(config.getString(config_root + '.' + key));
    }
    holder.prepare();
    return holder;
}

bool dataToBlock(const void * data, Block & block)
{
    if (!data)
        return true;
    auto columns_recd = static_cast<ClickHouseLib::ColumnsUInt64 *>(data);
    std::vector<IColumn *> columns(block.columns());
    for (const auto i : ext::range(0, columns.size()))
        columns[i] = block.getByPosition(i).column.get();
    for (size_t i = 0; i < columns_recd->size; ++i)
    {
        if (columns.size() != columns_recd->data[i].size)
            throw Exception("Received unexpected number of columns " + std::to_string(columns_recd->data[i].size) + "/"
                    + std::to_string(columns.size()) /* + " in " + toString()*/,
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        for (size_t ii = 0; ii < columns_recd->data[i].size; ++ii)
        {
            columns[ii]->insert(columns_recd->data[i].data[ii]);
        }
    }
    return false;
}

LibDictionarySource::LibDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Block & sample_block,
    const Context & context)
    : log(&Logger::get("LibDictionarySource")),
      dict_struct{dict_struct_},
      config_prefix{config_prefix},
      filename{config.getString(config_prefix + ".filename", "")},
      sample_block{sample_block},
      context(context)
{
    if (!Poco::File(filename).exists())
    {
        throw Exception(
            "LibDictionarySource: Cant load lib " + toString() + " : " + Poco::File(filename).path(), ErrorCodes::FILE_DOESNT_EXIST);
    }
    description.init(sample_block);
    library = std::make_shared<SharedLibrary>(filename);
    settings = std::make_shared<CStringsHolder>(getLibSettings(config, config_prefix + lib_config_settings));
}

LibDictionarySource::LibDictionarySource(const LibDictionarySource & other)
    : log(&Logger::get("LibDictionarySource")),
      dict_struct{other.dict_struct},
      config_prefix{other.config_prefix},
      filename{other.filename},
      sample_block{other.sample_block},
      context(other.context)
{
}

BlockInputStreamPtr LibDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());

    auto columns_holder = std::make_unique<ClickHouseLib::CString[]>(dict_struct.attributes.size());
    ClickHouseLib::CStrings columns{
        dict_struct.attributes.size(), reinterpret_cast<decltype(ClickHouseLib::CStrings::data)>(columns_holder.get())};
    size_t i = 0;
    for (auto & a : dict_struct.attributes)
    {
        columns.data[i] = a.name.c_str();
        ++i;
    }
    void * data_ptr = nullptr;
    auto fptr = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns))>("loadAll");
    if (!fptr)
        throw Exception("Method loadAll not implemented in library " + toString(), ErrorCodes::NOT_IMPLEMENTED);
    data_ptr = library->get<void * (*)()>("dataAllocate")();
    auto data = fptr(data_ptr, &settings->strings, &columns);
    auto block = description.sample_block.cloneEmpty();
    dataToBlock(data, block);
    library->get<void (*)(void *)>("dataDelete")(data_ptr);
    return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());

    const ClickHouseLib::VectorUInt64 ids_data{ids.size(), ids.data()};
    auto columns_holder = std::make_unique<ClickHouseLib::CString[]>(dict_struct.attributes.size());
    ClickHouseLib::CStrings columns_pass{
        dict_struct.attributes.size(), reinterpret_cast<decltype(ClickHouseLib::CStrings::data)>(columns_holder.get())};
    size_t i = 0;
    for (auto & a : dict_struct.attributes)
    {
        columns_pass.data[i] = a.name.c_str();
        ++i;
    }
    void * data_ptr = nullptr;
    auto fptr = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns_pass), decltype(&ids_data))>(
        "loadIds");
    if (!fptr)
        throw Exception("Method loadIds not implemented in library " + toString(), ErrorCodes::NOT_IMPLEMENTED);
    data_ptr = library->get<void * (*)()>("dataAllocate")();
    auto data = fptr(data_ptr, &settings->strings, &columns_pass, &ids_data);
    auto block = description.sample_block.cloneEmpty();
    dataToBlock(data, block);
    library->get<void (*)(void * data_ptr)>("dataDelete")(data_ptr);
    return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibDictionarySource::loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());

    /*
    auto columns_c = std::make_unique<ClickHouseLib::Columns>(key_columns.size() + 1);
    size_t i = 0;
    for (auto & column : key_columns)
    {
        columns_c[i] = column->getName().c_str();
        ++i;
    }
    columns_c[i] = nullptr;
*/
    auto columns_holder = std::make_unique<ClickHouseLib::CString[]>(key_columns.size());
    ClickHouseLib::CStrings columns_pass{
        key_columns.size(), reinterpret_cast<decltype(ClickHouseLib::CStrings::data)>(columns_holder.get())};
    size_t key_columns_n = 0;
    for (auto & column : key_columns)
    {
        columns_pass.data[key_columns_n] = column->getName().c_str();
        ++key_columns_n;
    }
    const ClickHouseLib::VectorUInt64 requested_rows_c{requested_rows.size(), requested_rows.data()};
    void * data_ptr = nullptr;
    auto fptr
        = library->get<void * (*)(decltype(data_ptr), decltype(&settings->strings), decltype(&columns_pass), decltype(&requested_rows_c))>(
            "loadKeys");
    if (!fptr)
        throw Exception("Method loadKeys not implemented in library " + toString(), ErrorCodes::NOT_IMPLEMENTED);
    data_ptr = library->get<void * (*)()>("dataAllocate")();
    auto data = fptr(data_ptr, &settings->strings, &columns_pass, &requested_rows_c);
    auto block = description.sample_block.cloneEmpty();
    dataToBlock(data, block);
    library->get<void (*)(void * data_ptr)>("dataDelete")(data_ptr);
    return std::make_shared<OneBlockInputStream>(block);
}

bool LibDictionarySource::isModified() const
{
    auto fptr = library->get<void * (*)(decltype(&settings->strings))>("isModified", true);
    if (fptr)
        return fptr(&settings->strings);
    return true;
}

bool LibDictionarySource::supportsSelectiveLoad() const
{
    auto fptr = library->get<void * (*)(decltype(&settings->strings))>("supportsSelectiveLoad", true);
    if (fptr)
        return fptr(&settings->strings);
    return true;
}

DictionarySourcePtr LibDictionarySource::clone() const
{
    return std::make_unique<LibDictionarySource>(*this);
}

std::string LibDictionarySource::toString() const
{
    return filename;
}
}
