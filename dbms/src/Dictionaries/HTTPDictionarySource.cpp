#include <Dictionaries/HTTPDictionarySource.h>

#include <Poco/Net/HTTPRequest.h>
#include <Interpreters/Context.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromOStream.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <common/logger_useful.h>


namespace DB
{

static const size_t max_block_size = 8192;


HTTPDictionarySource::HTTPDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    Block & sample_block, const Context & context)
    : log(&Logger::get("HTTPDictionarySource")),
    dict_struct{dict_struct_},
    url{config.getString(config_prefix + ".url", "")},
    format{config.getString(config_prefix + ".format")},
    sample_block{sample_block},
    context(context)
{
}

HTTPDictionarySource::HTTPDictionarySource(const HTTPDictionarySource & other)
    : log(&Logger::get("HTTPDictionarySource")),
    dict_struct{other.dict_struct},
    url{other.url},
    format{other.format},
    sample_block{other.sample_block},
    context(other.context)
{
}

BlockInputStreamPtr HTTPDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll " + toString());
    Poco::URI uri(url);
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_GET);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds " << toString() << " size = " << ids.size());

    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & ostr)
    {
        WriteBufferFromOStream out_buffer(ostr);
        auto output_stream = context.getOutputFormat(format, out_buffer, sample_block);
        formatIDs(output_stream, ids);
    };

    Poco::URI uri(url);
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadKeys(
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

bool HTTPDictionarySource::isModified() const
{
    return true;
}

bool HTTPDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

DictionarySourcePtr HTTPDictionarySource::clone() const
{
    return std::make_unique<HTTPDictionarySource>(*this);
}

std::string HTTPDictionarySource::toString() const
{
    Poco::URI uri(url);
    return uri.toString();
}

}
