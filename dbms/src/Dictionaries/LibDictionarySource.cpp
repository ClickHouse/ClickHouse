#include <Dictionaries/LibDictionarySource.h>

#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>
#include <common/logger_useful.h>

#include <Interpreters/Compiler.h>

//dev:
#include <DataStreams/NullBlockInputStream.h>

namespace DB
{

//static const size_t max_block_size = 8192;


LibDictionarySource::LibDictionarySource(const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
        Block & sample_block, const Context & context)
    : log(&Logger::get("LibDictionarySource")),
      dict_struct {dict_struct_},
              filename {config.getString(config_prefix + ".filename", "")},
              format {config.getString(config_prefix + ".format")},
              sample_block {sample_block},
              context(context)
{
    std::cerr << "LibDictionarySource::LibDictionarySource()\n";
}

LibDictionarySource::LibDictionarySource(const LibDictionarySource & other)
    : log(&Logger::get("LibDictionarySource")),
      dict_struct {other.dict_struct},
              format {other.format},
              sample_block {other.sample_block},
              context(other.context)
{
}

BlockInputStreamPtr LibDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());
    auto lib = std::make_shared<SharedLibrary>(filename);
    auto fptr = lib->get<void * (*) ()>("loadAll");
    std::cerr << "LibDictionarySource::loadAll filename=" << filename << " fptr=" << fptr << "\n";
    if (fptr)
        fptr();

    return std::make_shared<NullBlockInputStream>();
}

BlockInputStreamPtr LibDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());

    auto lib = std::make_shared<SharedLibrary>(filename);
    auto fptr = lib->get<void * (*) (const std::vector<UInt64> &)>("loadIds");
    std::cerr << "LibDictionarySource::loadIds filename=" << filename << " size=" << ids.size()<< " fptr=" << fptr<< "\n";
    if (fptr)
        fptr(ids);

    return std::make_shared<NullBlockInputStream>();
}

BlockInputStreamPtr LibDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());

    auto lib = std::make_shared<SharedLibrary>(filename);
    auto fptr = lib->get<void * (*) (const std::vector<std::size_t> &)>("loadKeys");
    std::cerr << "LibDictionarySource::loadKeys filename=" << filename << " size=" << requested_rows.size() << " fptr=" << fptr << "\n";
    if (fptr)
        fptr(requested_rows);

    return std::make_shared<NullBlockInputStream>();
}

bool LibDictionarySource::isModified() const
{
    return true;
}

bool LibDictionarySource::supportsSelectiveLoad() const
{
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
