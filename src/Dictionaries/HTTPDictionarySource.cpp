#include "HTTPDictionarySource.h"
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/OwningBlockInputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Poco/Net/HTTPRequest.h>
#include <common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionarySourceHelpers.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
static const UInt64 max_block_size = 8192;


HTTPDictionarySource::HTTPDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    Block & sample_block_,
    const Context & context_,
    bool check_config)
    : log(&Poco::Logger::get("HTTPDictionarySource"))
    , update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , url{config.getString(config_prefix + ".url", "")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , format{config.getString(config_prefix + ".format")}
    , sample_block{sample_block_}
    , context(context_)
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context))
{

    if (check_config)
        context.getRemoteHostFilter().checkURL(Poco::URI(url));

    const auto & credentials_prefix = config_prefix + ".credentials";

    if (config.has(credentials_prefix))
    {
        credentials.setUsername(config.getString(credentials_prefix + ".user", ""));
        credentials.setPassword(config.getString(credentials_prefix + ".password", ""));
    }

    const auto & headers_prefix = config_prefix + ".headers";

    if (config.has(headers_prefix))
    {
        Poco::Util::AbstractConfiguration::Keys config_keys;
        config.keys(headers_prefix, config_keys);

        header_entries.reserve(config_keys.size());
        for (const auto & key : config_keys)
        {
            const auto header_key = config.getString(headers_prefix + "." + key + ".name", "");
            const auto header_value = config.getString(headers_prefix + "." + key + ".value", "");
            header_entries.emplace_back(std::make_tuple(header_key, header_value));
        }
    }
}

HTTPDictionarySource::HTTPDictionarySource(const HTTPDictionarySource & other)
    : log(&Poco::Logger::get("HTTPDictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , url{other.url}
    , header_entries{other.header_entries}
    , update_field{other.update_field}
    , format{other.format}
    , sample_block{other.sample_block}
    , context(other.context)
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context))
{
    credentials.setUsername(other.credentials.getUsername());
    credentials.setPassword(other.credentials.getPassword());
}

void HTTPDictionarySource::getUpdateFieldAndDate(Poco::URI & uri)
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        auto tmp_time = update_time;
        update_time = std::chrono::system_clock::now();
        time_t hr_time = std::chrono::system_clock::to_time_t(tmp_time) - 1;
        WriteBufferFromOwnString out;
        writeDateTimeText(hr_time, out);
        uri.addQueryParameter(update_field, out.str());
    }
    else
    {
        update_time = std::chrono::system_clock::now();
    }
}

BlockInputStreamPtr HTTPDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll {}", toString());
    Poco::URI uri(url);
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(
        uri, Poco::Net::HTTPRequest::HTTP_GET, ReadWriteBufferFromHTTP::OutStreamCallback(), timeouts,
        0, credentials, DBMS_DEFAULT_BUFFER_SIZE, header_entries);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadUpdatedAll()
{
    Poco::URI uri(url);
    getUpdateFieldAndDate(uri);
    LOG_TRACE(log, "loadUpdatedAll {}", uri.toString());
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(
        uri, Poco::Net::HTTPRequest::HTTP_GET, ReadWriteBufferFromHTTP::OutStreamCallback(), timeouts,
        0, credentials, DBMS_DEFAULT_BUFFER_SIZE, header_entries);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & ostr)
    {
        WriteBufferFromOStream out_buffer(ostr);
        auto output_stream = context.getOutputFormat(format, out_buffer, sample_block);
        formatIDs(output_stream, ids);
    };

    Poco::URI uri(url);
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(
        uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback, timeouts,
        0, credentials, DBMS_DEFAULT_BUFFER_SIZE, header_entries);
    auto input_stream = context.getInputFormat(format, *in_ptr, sample_block, max_block_size);
    return std::make_shared<OwningBlockInputStream<ReadWriteBufferFromHTTP>>(input_stream, std::move(in_ptr));
}

BlockInputStreamPtr HTTPDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [&](std::ostream & ostr)
    {
        WriteBufferFromOStream out_buffer(ostr);
        auto output_stream = context.getOutputFormat(format, out_buffer, sample_block);
        formatKeys(dict_struct, output_stream, key_columns, requested_rows);
    };

    Poco::URI uri(url);
    auto in_ptr = std::make_unique<ReadWriteBufferFromHTTP>(
        uri, Poco::Net::HTTPRequest::HTTP_POST, out_stream_callback, timeouts,
        0, credentials, DBMS_DEFAULT_BUFFER_SIZE, header_entries);
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

bool HTTPDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
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

void registerDictionarySourceHTTP(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context,
                                 const std::string & /* default_database */,
                                 bool check_config) -> DictionarySourcePtr
    {
        if (dict_struct.has_expressions)
            throw Exception{"Dictionary source of type `http` does not support attribute expressions", ErrorCodes::LOGICAL_ERROR};

        Context context_local_copy = copyContextAndApplySettings(config_prefix, context, config);

        return std::make_unique<HTTPDictionarySource>(
            dict_struct, config, config_prefix + ".http",
            sample_block, context_local_copy, check_config);
    };
    factory.registerSource("http", create_table_source);
}

}
