#include <Dictionaries/LibDictionarySource.h>

#include <Poco/Net/HTTPRequest.h>
#include <Interpreters/Context.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromOStream.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <common/logger_useful.h>

#include <Interpreters/Compiler.h>


namespace DB
{

static const size_t max_block_size = 8192;


LibDictionarySource::LibDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    Block & sample_block, const Context & context)
    : log(&Logger::get("LibDictionarySource")),
    dict_struct{dict_struct_},
    filename{config.getString(config_prefix + ".filename", "")},
    //url{config.getString(config_prefix + ".url", "")}, //del
    format{config.getString(config_prefix + ".format")},
    sample_block{sample_block},
    context(context)
{
    std::cerr << "LibDictionarySource::LibDictionarySource()\n";
    
}

LibDictionarySource::LibDictionarySource(const LibDictionarySource & other)
    : log(&Logger::get("LibDictionarySource")),
    dict_struct{other.dict_struct},
    url{other.url},
    format{other.format},
    sample_block{other.sample_block},
    context(other.context)
{
}

BlockInputStreamPtr LibDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());
    Poco::URI uri(url);
    std::cerr << "1dl filename=" << filename << "\n";
    SharedLibraryPtr lib = std::make_shared<SharedLibrary>(filename);

    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_GET);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
}

BlockInputStreamPtr LibDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());

    std::cerr << "2dl filename=" << filename << "\n";
    SharedLibraryPtr lib = std::make_shared<SharedLibrary>(filename);
    //auto fptr = lib->get<void * (*) ()>("_ZN2DB6getPtrEv")();
    auto fptr = lib->get<void * (*) (const std::vector<UInt64> &)>("loadIds");
    //(10);
    // libfunc
    //auto f = std::function(fptr);
    //auto f = std::mem_fn(&fptr);
    //f();
std::cerr << " fptr=" << fptr << "\n";

        if (fptr)
        {
            //reinterpret_cast<void (*)(const Aggregator &, AggregatedDataWithoutKey &, size_t, AggregateColumns &, Arena *)>
            //        (f)(*this, result.without_key, rows, aggregate_columns, result.aggregates_pool);
            //reinterpret_cast<void (*)(int)> (fptr)(11);
fptr(ids);
std::cerr << " fptr destroy.."  << "\n";
        }


    //fptr();

std::cerr << " fptr done." << "\n";

    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & ostr)
    {
        WriteBufferFromOStream out_buffer(ostr);
        auto output_stream = context.getOutputFormat(format, out_buffer, sample_block);
        formatIDs(output_stream, ids);
    };

    Poco::URI uri(url);
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);


std::cerr << "fptr destroy2:" << "\n";

    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
}

BlockInputStreamPtr LibDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys " << toString() << " size = " << requested_rows.size());

    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & ostr)
    {
        WriteBufferFromOStream out_buffer(ostr);
        auto output_stream = context.getOutputFormat(format, out_buffer, sample_block);
        formatKeys(dict_struct, output_stream, key_columns, requested_rows);
    };

    Poco::URI uri(url);
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
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
    Poco::URI uri(url);
    return uri.toString();
}

}
