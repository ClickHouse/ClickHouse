#include "HTTPDictionarySource.h"
#include <Common/HTTPHeaderFilter.h>
#include <Core/ServerSettings.h>
#include <Formats/formatBlock.h>
#include <IO/ConnectionTimeouts.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IInputFormat.h>
#include <Poco/Net/HTTPRequest.h>
#include <Common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionarySourceHelpers.h"
#include "DictionaryStructure.h"
#include <Storages/NamedCollectionsHelpers.h>
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
    const Configuration & configuration_,
    const Poco::Net::HTTPBasicCredentials & credentials_,
    Block & sample_block_,
    ContextPtr context_)
    : log(getLogger("HTTPDictionarySource"))
    , update_time(std::chrono::system_clock::from_time_t(0))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , context(context_)
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
{
    credentials.setUsername(credentials_.getUsername());
    credentials.setPassword(credentials_.getPassword());
}

HTTPDictionarySource::HTTPDictionarySource(const HTTPDictionarySource & other)
    : log(getLogger("HTTPDictionarySource"))
    , update_time(other.update_time)
    , dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , sample_block(other.sample_block)
    , context(Context::createCopy(other.context))
    , timeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
{
    credentials.setUsername(other.credentials.getUsername());
    credentials.setPassword(other.credentials.getPassword());
}

QueryPipeline HTTPDictionarySource::createWrappedBuffer(std::unique_ptr<ReadWriteBufferFromHTTP> http_buffer_ptr)
{
    Poco::URI uri(configuration.url);
    String http_request_compression_method_str = http_buffer_ptr->getCompressionMethod();
    auto in_ptr_wrapped
        = wrapReadBufferWithCompressionMethod(std::move(http_buffer_ptr), chooseCompressionMethod(uri.getPath(), http_request_compression_method_str));
    auto source = context->getInputFormat(configuration.format, *in_ptr_wrapped, sample_block, max_block_size);
    source->addBuffer(std::move(in_ptr_wrapped));
    return QueryPipeline(std::move(source));
}

void HTTPDictionarySource::getUpdateFieldAndDate(Poco::URI & uri)
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        auto tmp_time = update_time;
        update_time = std::chrono::system_clock::now();
        time_t hr_time = std::chrono::system_clock::to_time_t(tmp_time) - configuration.update_lag;
        WriteBufferFromOwnString out;
        writeDateTimeText(hr_time, out);
        uri.addQueryParameter(configuration.update_field, out.str());
    }
    else
    {
        update_time = std::chrono::system_clock::now();
    }
}

QueryPipeline HTTPDictionarySource::loadAll()
{
    LOG_TRACE(log, "loadAll {}", toString());

    Poco::URI uri(configuration.url);

    auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withSettings(context->getReadSettings())
                   .withTimeouts(timeouts)
                   .withHeaders(configuration.header_entries)
                   .withDelayInit(false)
                   .create(credentials);

    return createWrappedBuffer(std::move(buf));
}

QueryPipeline HTTPDictionarySource::loadUpdatedAll()
{
    Poco::URI uri(configuration.url);
    getUpdateFieldAndDate(uri);
    LOG_TRACE(log, "loadUpdatedAll {}", uri.toString());

    auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withSettings(context->getReadSettings())
                   .withTimeouts(timeouts)
                   .withHeaders(configuration.header_entries)
                   .withDelayInit(false)
                   .create(credentials);

    return createWrappedBuffer(std::move(buf));
}

QueryPipeline HTTPDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    LOG_TRACE(log, "loadIds {} size = {}", toString(), ids.size());

    auto block = blockForIds(dict_struct, ids);

    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [block, this](std::ostream & ostr)
    {
        WriteBufferFromOStream out_buffer(ostr);
        auto output_format = context->getOutputFormatParallelIfPossible(configuration.format, out_buffer, block.cloneEmpty());
        formatBlock(output_format, block);
        out_buffer.finalize();
    };

    Poco::URI uri(configuration.url);

    auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withSettings(context->getReadSettings())
                   .withTimeouts(timeouts)
                   .withHeaders(configuration.header_entries)
                   .withOutCallback(std::move(out_stream_callback))
                   .withDelayInit(false)
                   .create(credentials);

    return createWrappedBuffer(std::move(buf));
}

QueryPipeline HTTPDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    LOG_TRACE(log, "loadKeys {} size = {}", toString(), requested_rows.size());

    auto block = blockForKeys(dict_struct, key_columns, requested_rows);

    ReadWriteBufferFromHTTP::OutStreamCallback out_stream_callback = [block, this](std::ostream & ostr)
    {
        WriteBufferFromOStream out_buffer(ostr);
        auto output_format = context->getOutputFormatParallelIfPossible(configuration.format, out_buffer, block.cloneEmpty());
        formatBlock(output_format, block);
        out_buffer.finalize();
    };

    Poco::URI uri(configuration.url);

    auto buf = BuilderRWBufferFromHTTP(uri)
                   .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                   .withMethod(Poco::Net::HTTPRequest::HTTP_POST)
                   .withSettings(context->getReadSettings())
                   .withTimeouts(timeouts)
                   .withHeaders(configuration.header_entries)
                   .withOutCallback(std::move(out_stream_callback))
                   .withDelayInit(false)
                   .create(credentials);

    return createWrappedBuffer(std::move(buf));
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
    return !configuration.update_field.empty();
}

DictionarySourcePtr HTTPDictionarySource::clone() const
{
    return std::make_shared<HTTPDictionarySource>(*this);
}

std::string HTTPDictionarySource::toString() const
{
    Poco::URI uri(configuration.url);
    return uri.toString();
}

void registerDictionarySourceHTTP(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                   const Poco::Util::AbstractConfiguration & config,
                                   const std::string & config_prefix,
                                   Block & sample_block,
                                   ContextPtr global_context,
                                   const std::string & /* default_database */,
                                   bool created_from_ddl) -> DictionarySourcePtr {
        if (dict_struct.has_expressions)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Dictionary source of type `http` does not support attribute expressions");

        auto settings_config_prefix = config_prefix + ".http";
        Poco::Net::HTTPBasicCredentials credentials;
        HTTPHeaderEntries header_entries;
        String url;
        String endpoint;
        String format;

        auto named_collection = created_from_ddl ? tryGetNamedCollectionWithOverrides(config, settings_config_prefix, global_context) : nullptr;
        if (named_collection)
        {
            validateNamedCollection(
                *named_collection,
                /* required_keys */{},
                /* optional_keys */ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>{
                "url", "endpoint", "user", "credentials.user", "password", "credentials.password", "format", "compression_method", "structure", "name"});

            url = named_collection->getOrDefault<String>("url", "");
            endpoint = named_collection->getOrDefault<String>("endpoint", "");
            format = named_collection->getOrDefault<String>("format", "");

            credentials.setUsername(named_collection->getAnyOrDefault<String>({"user", "credentials.user"}, ""));
            credentials.setPassword(named_collection->getAnyOrDefault<String>({"password", "credentials.password"}, ""));

            header_entries = getHeadersFromNamedCollection(*named_collection);
        }
        else
        {
            const auto & credentials_prefix = settings_config_prefix + ".credentials";

            if (config.has(credentials_prefix))
            {
                credentials.setUsername(config.getString(credentials_prefix + ".user", ""));
                credentials.setPassword(config.getString(credentials_prefix + ".password", ""));
            }

            const auto & headers_prefix = settings_config_prefix + ".headers";

            if (config.has(headers_prefix))
            {
                Poco::Util::AbstractConfiguration::Keys config_keys;
                config.keys(headers_prefix, config_keys);

                header_entries.reserve(config_keys.size());
                for (const auto & key : config_keys)
                {
                    const auto header_key = config.getString(headers_prefix + "." + key + ".name", "");
                    const auto header_value = config.getString(headers_prefix + "." + key + ".value", "");
                    header_entries.emplace_back(header_key, header_value);
                }
            }

            url = config.getString(settings_config_prefix + ".url", "");
            endpoint = config.getString(settings_config_prefix + ".endpoint", "");
            format =config.getString(settings_config_prefix + ".format", "");
        }

        if (url.ends_with('/'))
        {
            if (endpoint.starts_with('/'))
                url.pop_back();
        }
        else if (!endpoint.empty() && !endpoint.starts_with('/'))
            url.push_back('/');

        auto configuration = HTTPDictionarySource::Configuration
        {
            .url = url + endpoint,
            .format = format,
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
            .header_entries = std::move(header_entries)
        };

        auto context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        if (created_from_ddl)
        {
            context->getRemoteHostFilter().checkURL(Poco::URI(configuration.url));
            context->getHTTPHeaderFilter().checkHeaders(configuration.header_entries);
        }

        return std::make_unique<HTTPDictionarySource>(dict_struct, configuration, credentials, sample_block, context);
    };
    factory.registerSource("http", create_table_source);
}

}
