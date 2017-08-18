#include <Dictionaries/LibDictionarySource.h>
#include "LibDictionarySourceExternal.h"

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <Poco/File.h>

//dev:
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/iostream_debug_helpers.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/OneBlockInputStream.h>


namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DB
{
//static const size_t max_block_size = 8192;


    const std::string lib_config_settings = ".settings";
    
struct StringHolder
{
    ClickhouseStrings strings; // will pass pointer to lib
    std::unique_ptr<ClickhouseString[]> ptrHolder = nullptr;
    std::vector<std::string> stringHolder;

    //ClickhouseSettings *
    void prepare()
    {
        strings.size = stringHolder.size();
        //return &settings;
        ptrHolder = std::make_unique<ClickhouseString[]>(strings.size);
        strings.data = ptrHolder.get();
        size_t i = 0;
        for (auto & str : stringHolder)
        {
            //DUMP(i);        DUMP(a.name);        DUMP(a.type);
            strings.data[i] = str.c_str();
            ++i;
        }
    }
};
StringHolder getLibSettings(const Poco::Util::AbstractConfiguration & config, const std::string & config_root
    //, const std::string & config_name

    )
{
    StringHolder holder;
    /*
        auto valuesk = getMultipleValuesFromConfig(config, config_root, config_name);
        std::cerr << "config valuesK: ";
        DUMP(valuesk);
        
        auto values = getMultipleKeysFromConfig(config, config_root, config_name);
        std::cerr << "config values: ";
        DUMP(values);
        */
    //DUMP("");
    //holder.stringHolder.clear();
    Poco::Util::AbstractConfiguration::Keys config_keys;
    //DUMP("");
    //throw;
    config.keys(config_root, config_keys);
    //DUMP("");
    //std::cerr << "keys root " << config_root << " = " << config_keys << "\n";
    for (const auto & key : config_keys)
    {
        //std::cerr << "cmp " << key << " " << name  << "\n";
        //std::cerr << "cmp1 " << key  << "\n";
        std::string key_name = key;
        auto bracket_pos = key.find('[');
        if (bracket_pos != std::string::npos && bracket_pos > 0)
            key_name = key.substr(0, bracket_pos);
        //std::cerr << "cmp2 " << key  << " : "<<key_name<< "\n";
        holder.stringHolder.emplace_back(key_name);

        holder.stringHolder.emplace_back(config.getString(config_root + '.' + key));
    }

    //holder.stringHolder = values;

    //DUMP("");
    holder.prepare();
    //DUMP("");
    return holder;
}

bool dataToBlock(void * data, Block & block)
{
    if (!data)
        return true;

    std::vector<IColumn *> columns(block.columns());
    for (const auto i : ext::range(0, columns.size()))
        columns[i] = block.getByPosition(i).column.get();
    DUMP(columns.size());
    //std::cerr << "bl clean=" << block << "\n";

    auto columns_recd = static_cast<ClickhouseColumnsUint64 *>(data);
    DUMP(columns_recd->size);
    for (size_t i = 0; i < columns_recd->size; ++i)
    {
        DUMP(i);
        DUMP(columns_recd->data[i].size);
        DUMP(columns_recd->data[i].data);
        //DUMP("ONE:");
        for (size_t ii = 0; ii < columns_recd->data[i].size; ++ii)
        {
            DUMP(ii);
            DUMP(columns_recd->data[i].data[ii]);
            columns[ii]->insert(columns_recd->data[i].data[ii]);
        }
    }
    return false;
}


//struct LoadIdsParams {const uint64_t size; const uint64_t * data;};

LibDictionarySource::LibDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config_,
    const std::string & config_prefix_,
    Block & sample_block,
    const Context & context)
    : log(&Logger::get("LibDictionarySource")),
      dict_struct{dict_struct_},
      config{config_},
      config_prefix{config_prefix_},
      filename{config.getString(config_prefix + ".filename", "")},
      //format {config.getString(config_prefix + ".format")},
      sample_block{sample_block},
      context(context)
{
    std::cerr << "LibDictionarySource::LibDictionarySource()\n";
    std::cerr << "config_prefix=" << config_prefix << "\n";
    if (!Poco::File(filename).exists())
    {
        LOG_ERROR(log, "LibDictionarySource: Cant load lib " << toString() << " : " << Poco::File(filename).path());
        //throw;
    }
    description.init(sample_block);
    library = std::make_shared<SharedLibrary>(filename);
}

LibDictionarySource::LibDictionarySource(const LibDictionarySource & other)
    : log(&Logger::get("LibDictionarySource")),
      dict_struct{other.dict_struct},
      config{other.config},
      config_prefix{other.config_prefix},
      filename{other.filename},
      //format {other.format},
      sample_block{other.sample_block},
      context(other.context)
{
}

BlockInputStreamPtr LibDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());

    //for (auto & a : dict_struct.attributes) { DUMP(a.name); DUMP(a.type); }

    //auto lib = std::make_shared<SharedLibrary>(filename);
    //auto fptr = lib->get<void * (*) ()>("loadAll");

    auto columns_holder = std::make_unique<ClickhouseString[]>(dict_struct.attributes.size());
    ClickhouseStrings columns{dict_struct.attributes.size(), reinterpret_cast<decltype(ClickhouseStrings::data)>(columns_holder.get())};
    size_t i = 0;
    for (auto & a : dict_struct.attributes)
    {
        DUMP(i);        DUMP(a.name);        DUMP(a.type);
        columns.data[i] = a.name.c_str();
        ++i;
    }

    //DUMP(config_prefix);
    //DUMP(config_prefix + ".lib");
    auto settings = getLibSettings(config, config_prefix + lib_config_settings);
    void * data_ptr = nullptr;

    auto fptr = library->get<void * (*)(decltype(data_ptr), decltype(&settings.strings), decltype(&columns))>("loadAll");

    if (!fptr)
        //return std::make_shared<NullBlockInputStream>();
        throw Exception("method loadAll not implemented in library " + toString(), ErrorCodes::NOT_IMPLEMENTED);

    data_ptr = library->get<void * (*)()>("dataAllocate")();

    auto data = fptr(data_ptr, &settings.strings, &columns);
    //DUMP(data);

    auto block = description.sample_block.cloneEmpty();
    dataToBlock(data, block);

    /*    auto columns_recieved = static_cast<ClickhouseColumnsUint64 *>(data);
    if (data)
    {
        DUMP(columns_recieved->size);
    }*/
    // TODO
    library->get<void (*)(void *)>("dataDelete")(data_ptr);

    //std::cerr << "LibDictionarySource::loadAll filename=" << filename << " fptr=" << fptr << "\n";
    //if (fptr)
    //    fptr();

    //return std::make_shared<OneBlockInputStream>(std::move(Block()));
    //auto block = description.sample_block.cloneEmpty();
    //return std::make_shared<OneBlockInputStream>(std::move(block));

    return std::make_shared<OneBlockInputStream>(block);
}


BlockInputStreamPtr LibDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());

    //DUMP(key_columns);
    //ClickhouseStrings
    //struct {const uint64_t size; const uint64_t * data;}
    const ClickhouseVectorUint64 ids_data{ids.size(), ids.data()};
    //c_data.size = ids.size();
    //for (size_t i = 0; i <= ids.size(); ++i) {
    //  data[i] = ids[i];
    //}
    auto columns_holder = std::make_unique<ClickhouseString[]>(dict_struct.attributes.size());
    ClickhouseStrings columns_pass{
        dict_struct.attributes.size(), reinterpret_cast<decltype(ClickhouseStrings::data)>(columns_holder.get())};
        DUMP2("alloc size=", dict_struct.attributes.size());
        size_t i = 0;
    for (auto & a : dict_struct.attributes)
    {
        DUMP(i); DUMP(a.name); DUMP(a.type);
        columns_pass.data[i] = a.name.c_str();
        //++columns.size;
        ++i;
    }

    //DUMP("");
    auto settings = getLibSettings(config, config_prefix + lib_config_settings);
    //DUMP("");
    
    //auto lib = std::make_shared<SharedLibrary>(filename);
    //auto data_ptr = library->get<void * (*) ()>("dataAllocate")();
    void * data_ptr = nullptr;
    //DUMP("");
    
    auto fptr = library->get<void * (*)(decltype(data_ptr), decltype(&settings.strings), decltype(&columns_pass), decltype(&ids_data))>(
        "loadIds");
    //DUMP("");
    
    if (!fptr)
        throw Exception("method loadIds not implemented in library " + toString(), ErrorCodes::NOT_IMPLEMENTED);
    //return std::make_shared<NullBlockInputStream>();

    data_ptr = library->get<void * (*)()>("dataAllocate")();

    auto data = fptr(data_ptr, &settings.strings, &columns_pass, &ids_data);
    //DUMP(data);


    auto block = description.sample_block.cloneEmpty();
    dataToBlock(data, block);


    library->get<void (*)(void * data_ptr)>("dataDelete")(data_ptr);

    /*auto fptr = lib->get<void * (*) (const std::vector<UInt64> &)>("loadIds");
    std::cerr << "LibDictionarySource::loadIds filename=" << filename << " size=" << ids.size()<< " fptr=" << fptr<< "\n";
    if (fptr)
        fptr(ids);
    */

    //return std::make_shared<NullBlockInputStream>();
    //return std::make_shared<OneBlockInputStream>(std::move(Block()));
    std::cerr << "blout=" << block << "\n";
    return std::make_shared<OneBlockInputStream>(std::move(block));
    //return std::make_shared<OneBlockInputStream>(block);
}

BlockInputStreamPtr LibDictionarySource::loadKeys(const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());
    //auto lib = std::make_shared<SharedLibrary>(filename);

    //std::cerr << Columns << "\n";
    DUMP(key_columns);
    // getName
    //auto col = new ClickhouseColumns[key_columns.size()];
    //auto columns_c = new const char*[key_columns.size()+1];
    //auto columns_c_container = std::make_shared<ClickhouseColumns>(new const char*[key_columns.size()+1]);
    //auto columns_c_container = std::make_unique<ClickhouseColumns>(new const char*[key_columns.size()+1]);
    //auto columns_c_container = std::make_unique<ClickhouseColumns>(key_columns.size()+1);
    //ClickhouseColumn* columns_c = columns_c_container.get();
    //auto columns_c = std::make_unique<ClickhouseColumns>(key_columns.size()+1);

    auto columns_c = std::make_unique<ClickhouseColumns>(key_columns.size() + 1);

    size_t i = 0;
    for (auto & column : key_columns)
    {
        // FIXME
        columns_c[i] = column->getName().c_str();
        std::cerr << "ptr" << i << "=" << (size_t)(columns_c[i]) << " T=" << typeid(columns_c[i]).name() << " s=" << column->getName()
                  << " pc=" << (size_t)column->getName().c_str() << " pcs=" << column->getName().c_str()
                  << " TF=" << typeid(column->getName().c_str()).name() << "\n";
        ++i;
    }
    columns_c[i] = nullptr;

    /*
    i = 0;
    ClickhouseColumn column;
    while ((column = columns_c[i++]))
    {
        std::cerr << "T column i=" << i << " = [" << column << "] p=" << (size_t)column << "\n";
    }*/

    auto settings = getLibSettings(config, config_prefix + lib_config_settings);
    const ClickhouseVectorUint64 requested_rows_c{requested_rows.size(), requested_rows.data()};

    void * data_ptr = nullptr;

    auto fptr
        = library
              ->get<void * (*)(decltype(data_ptr), decltype(&settings.strings), decltype(columns_c)::pointer, decltype(requested_rows_c))>(
                  "loadKeys");
    if (!fptr)
        throw Exception("method loadKeys not implemented in library " + toString(), ErrorCodes::NOT_IMPLEMENTED);
    //return std::make_shared<NullBlockInputStream>();


    data_ptr = library->get<void * (*)()>("dataAllocate")();

    //library->get<void * (*)(void *, ClickhouseColumnsUint64, decltype(params))>("loadKeys")(data_ptr, columns_c.get(), params);

    auto data = fptr(data_ptr, &settings.strings, columns_c.get(), requested_rows_c);


    // TODO
    auto block = description.sample_block.cloneEmpty();
    dataToBlock(data, block);

    library->get<void (*)(void * data_ptr)>("dataDelete")(data_ptr);

    //delete columns_c;
    //lib->get<void * (*) (const std::vector<std::size_t> &)>("loadKeys")(requested_rows);
    /* auto fptr = lib->get<void * (*) (const std::vector<std::size_t> &)>("loadKeys");
    std::cerr << "LibDictionarySource::loadKeys filename=" << filename << " size=" << requested_rows.size() << " fptr=" << fptr << "\n";
    if (fptr)
        fptr(requested_rows);
    */

    //return std::make_shared<NullBlockInputStream>();
    return std::make_shared<OneBlockInputStream>(Block());
}

bool LibDictionarySource::isModified() const
{
    //auto lib = std::make_shared<SharedLibrary>(filename);
    auto settings = getLibSettings(config, config_prefix + lib_config_settings);
    auto fptr = library->get<void * (*)(decltype(&settings.strings))>("isModified", true);
    if (fptr)
        return fptr(&settings.strings);
    //std::cerr << "no lib's isModified\n";
    return true;
}

bool LibDictionarySource::supportsSelectiveLoad() const
{
    //auto lib = std::make_shared<SharedLibrary>(filename);

    auto settings = getLibSettings(config, config_prefix + lib_config_settings);

    auto fptr = library->get<void * (*)(decltype(&settings.strings))>("supportsSelectiveLoad", true);
    if (fptr)
        return fptr(&settings.strings);
    //std::cerr << "no lib's supportsSelectiveLoad\n";
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
