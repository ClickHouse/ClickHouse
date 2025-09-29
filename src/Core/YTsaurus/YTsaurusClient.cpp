#include "config.h"
#if USE_YTSAURUS
#include <Core/YTsaurus/YTsaurusClient.h>

#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromOStream.h>
#include <Formats/formatBlock.h>
#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/JSON/Parser.h>
#include <Storages/ColumnsDescription.h>
#include <DataTypes/convertYTsaurusDataType.h>

#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/Pipe.h>


#include <memory>


namespace DB
{
namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
    extern const int ALL_CONNECTION_TRIES_FAILED;
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsUInt64 max_http_get_redirects;
}

YTsaurusClient::YTsaurusClient(ContextPtr context_, const ConnectionInfo & connection_info_)
    : context(context_), connection_info(connection_info_), log(getLogger("YTsaurusClient"))
{
}


ReadBufferPtr YTsaurusClient::readTable(const String & cypress_path)
{
    YTsaurusQueryPtr read_table_query(new YTsaurusReadTableQuery(cypress_path));
    return executeQuery(read_table_query);
}

YTsaurusNodeType YTsaurusClient::getNodeType(const String & cypress_path)
{
    auto json_ptr = getTableInfo(cypress_path);
    return getNodeTypeFromAttributes(json_ptr);
}


YTsaurusNodeType YTsaurusClient::getNodeTypeFromAttributes(const Poco::JSON::Object::Ptr & json_ptr)
{
    if (!json_ptr->has("type"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect json with yt attributes, no field 'type'.");

    if (json_ptr->getValue<String>("type") == "table")
    {
        if (!json_ptr->has("dynamic"))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect json with yt attributes, no field 'dynamic'.");

        return json_ptr->getValue<bool>("dynamic") ? YTsaurusNodeType::DYNAMIC_TABLE : YTsaurusNodeType::STATIC_TABLE;
    }
    else
    {
        return YTsaurusNodeType::ANOTHER;
    }
}

ReadBufferPtr YTsaurusClient::selectRows(const String & cypress_path)
{
    YTsaurusQueryPtr select_rows_query(new YTsaurusSelectRowsQuery(cypress_path));
    return executeQuery(select_rows_query);
}

ReadBufferPtr YTsaurusClient::lookupRows(const String & cypress_path, const Block & lookup_block_input)
{
    YTsaurusQueryPtr lookup_rows_query(new YTsaurusLookupRows(cypress_path));
    auto out_callback = [lookup_block_input, this](std::ostream & ostr)
    {
        FormatSettings format_settings;
        format_settings.json.quote_64bit_integers = false;
        WriteBufferFromOStream out_buffer(ostr);
        auto output_format = context->getOutputFormat("JSONEachRow", out_buffer, lookup_block_input.cloneEmpty(), format_settings);
        formatBlock(output_format, lookup_block_input);
        out_buffer.finalize();
    };
    return executeQuery(lookup_rows_query, std::move(out_callback));
}

ReadBufferPtr YTsaurusClient::executeQuery(const YTsaurusQueryPtr query, const ReadWriteBufferFromHTTP::OutStreamCallback&& out_callback)
{
    for (size_t num_try = 0; num_try < connection_info.http_proxy_urls.size(); ++num_try)
    {
        size_t url_index = (recently_used_url_index + num_try) % connection_info.http_proxy_urls.size();
        URI host_for_request(connection_info.http_proxy_urls[url_index].c_str());
        if (connection_info.enable_heavy_proxy_redirection && query->isHeavyQuery())
            host_for_request = getHeavyProxyURI(host_for_request);

        try
        {
            host_for_request.setPath(fmt::format("/api/{}/{}", connection_info.api_version, query->getQueryName()));

            for (const auto & query_param : query->getQueryParameters())
            {
                host_for_request.addQueryParameter(query_param.name, query_param.value);
            }
            LOG_TRACE(log, "URI {} , query type {}", host_for_request.toString(), query->getQueryName());

            auto buf = createQueryRWBuffer(host_for_request, out_callback, query->getHTTPMethod());
            recently_used_url_index = url_index;
            return buf;
        }
        catch (Exception & e)
        {
            LOG_WARNING(log, "Error while creating connection with {}, will try to use another http proxy if there are any. Exception: {}",
                connection_info.http_proxy_urls[url_index], e.displayText());
        }
    }
    throw Exception(ErrorCodes::ALL_CONNECTION_TRIES_FAILED, "All connection tries with ytsaurus http proxies are failed");
}

YTsaurusClient::URI YTsaurusClient::getHeavyProxyURI(const URI& uri)
{
    URI list_of_proxies_uri(uri);
    list_of_proxies_uri.setPath("/hosts");

    LOG_TRACE(log, "Get list of heavy proxies from path {}", list_of_proxies_uri.toString());
    auto buf = createQueryRWBuffer(list_of_proxies_uri, nullptr, "GET");

    // Make sure that there are no recursive calls
    String json_str;
    readJSONArrayInto(json_str, *buf);

    Poco::JSON::Parser parser;
    auto list_of_proxies = parser.parse(json_str).extract<Poco::JSON::Array::Ptr>();
    if (!list_of_proxies || !list_of_proxies->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't take the list of heavy proxies.");

    URI host;
    host.setScheme(uri.getScheme());
    host.setAuthority(list_of_proxies->getElement<std::string>(0));
    return host;
}


ReadBufferPtr YTsaurusClient::createQueryRWBuffer(const URI& uri, const ReadWriteBufferFromHTTP::OutStreamCallback& out_callback, const std::string & http_method)
{
    std::string output_params = "<uuid_mode=text_yql;complex_type_mode=positional>";
    HTTPHeaderEntries http_headers{
        /// Always use json format for input and output.
        {"Accept", "application/json"},
        {"Content-Type", "application/json"},
        {"Authorization", fmt::format("OAuth {}", connection_info.oauth_token)},
        {"X-YT-Header-Format", "<format=text>yson"},
        {"X-YT-Output-Format", fmt::format("{}json", output_params)},
    };

    Poco::Net::HTTPBasicCredentials creds;
    auto buf = BuilderRWBufferFromHTTP(uri)
                .withConnectionGroup(HTTPConnectionGroupType::STORAGE)
                .withMethod(http_method)
                .withSettings(context->getReadSettings())
                .withTimeouts(ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
                .withHostFilter(&context->getRemoteHostFilter())
                .withRedirects(context->getSettingsRef()[Setting::max_http_get_redirects])
                .withOutCallback(out_callback)
                .withHeaders(http_headers)
                .withDelayInit(false)
                .create(creds);

    return ReadBufferPtr(std::move(buf));
}

Poco::Dynamic::Var YTsaurusClient::getMetadata(const String & path)
{
    YTsaurusQueryPtr get_query(new YTsaurusGetQuery(path));
    auto buf = executeQuery(get_query);

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    return json;
}

Poco::JSON::Object::Ptr YTsaurusClient::getTableInfo(const String & cypress_path)
{
    String attributes_path = cypress_path + "/@";
    auto json = getMetadata(attributes_path);
    return json.extract<Poco::JSON::Object::Ptr>();
}

Poco::Dynamic::Var YTsaurusClient::getTableAttribute(const String & cypress_path, const String & attribute_name)
{
    String attribute_path = cypress_path + "/@" + attribute_name;
    auto json = getMetadata(attribute_path);
    return json;
}

Poco::JSON::Array::Ptr YTsaurusClient::getTableSchema(const String & cypress_path)
{
    auto schema = getTableAttribute(cypress_path, "schema");
    const auto & schema_json = schema.extract<Poco::JSON::Object::Ptr>();
    if (!schema_json->has("$value"))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No \"$value\" property in yt table schema");
    }
    return schema_json->get("$value").extract<Poco::JSON::Array::Ptr>();
}

bool YTsaurusClient::checkSchemaCompatibility(const String & table_path, const SharedHeader & sample_block)
{
    auto schema_json = getTableSchema(table_path);
    chassert(schema_json);
    for (const auto& yt_column : *schema_json) {
        try
        {
            const auto & yt_column_json = yt_column.extract<Poco::JSON::Object::Ptr>();
            auto yt_column_name = yt_column_json->getValue<String>("name");
            if (!sample_block->has(yt_column_name))
            {
                LOG_ERROR(log, "Table schema mismatch. No column {}", yt_column_name);
                return false;
            }

            const auto & column_type_ptr = sample_block->getByName(yt_column_name).type;

            chassert(column_type_ptr != nullptr);
            auto data_type = convertYTSchema(yt_column_json);
            if (column_type_ptr->getName() != "Dynamic" &&
                data_type->getName() != "Dynamic" &&
                column_type_ptr->getName() != data_type->getName())
            {
                LOG_ERROR(log, "Table schema mismatch. Clickhouse expecting: {}, Real: {}", column_type_ptr->getName(), data_type->getName());
                return false;
            }
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::INCORRECT_DATA)
            {
                LOG_DEBUG(log, "Couldn't extract schema from {}: {}", table_path, e.what());
                return false;
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Something went wrong while parsing YT table schema: {}", e.what());
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Something went wrong while parsing YT table schema: {}", e.what());
        }
    }
    return true;
}

}
#endif
