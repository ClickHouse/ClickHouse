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
#include <utility>


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

YTsaurusClient::YTsaurusClient(const YTsaurusClient & other)
    : context(other.context)
    , connection_info(other.connection_info)
    , log(getLogger("YTsaurusClient"))
{
}

ReadBufferPtr YTsaurusClient::readTable(const String & cypress_path, const std::pair<size_t, size_t> & rows_range)
{
    YTsaurusQueryPtr read_table_query(new YTsaurusReadTableQuery(cypress_path, rows_range));
    return executeQuery(read_table_query);
}

String YTsaurusClient::startTx(size_t timeout_ms)
{
    YTsaurusQueryPtr start_tx_query(new YTsaurusStartTxQuery(timeout_ms));
    auto read_buff = executeQuery(start_tx_query);
    String res;
    // Generally the result of each YTsaurus query should be json.
    // But... for start_tx query the result always double quoted string.
    readDoubleQuotedString(res, *read_buff);
    return res;
}

void YTsaurusClient::commitTx(const String & transaction_id)
{
    YTsaurusQueryPtr commit_tx_query(new YTsaurusCommitTxQuery(transaction_id));
    executeQuery(commit_tx_query);
}

String YTsaurusClient::lock(const String & cypress_path, const String & transaction_id)
{
    YTsaurusQueryPtr lock_query(new YTsaurusLockQuery(cypress_path, transaction_id));
    auto read_buff = executeQuery(lock_query);
    String res;
    // Generally the result of each YTsaurus query should be json.
    // But... for start_tx query the result always double quoted string.
    readDoubleQuotedString(res, *read_buff);
    return res;
}

String YTsaurusClient::getNodeIdFromLock(const String & lock_id)
{
    String lock_metadata_path = fmt::format("{}/{}/@node_id", LOCKS_STORAGE_CYPRESS_PATH, lock_id);
    YTsaurusQueryPtr get_query(new YTsaurusGetQuery(lock_metadata_path));
    auto read_buff = executeQuery(get_query);
    String res;
    readDoubleQuotedString(res, *read_buff);
    return res;
}

YTsaurusNodeType YTsaurusClient::getNodeType(const String & cypress_path)
{
    auto json_ptr = getNodeMetadata(cypress_path);
    return getNodeTypeFromAttributes(json_ptr);
}


YTsaurusNodeType YTsaurusClient::getNodeTypeFromAttributes(const Poco::JSON::Object::Ptr & json_ptr)
{
    if (!json_ptr->has("type"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect json with yt attributes, no field 'type'.");

    if (json_ptr->getValue<String>("type") == "table" || json_ptr->getValue<String>("type") == "replicated_table")
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

ReadBufferPtr YTsaurusClient::selectRows(const String & cypress_path, const String & column_names_str = "*")
{
    YTsaurusQueryPtr select_rows_query(new YTsaurusSelectRowsQuery(cypress_path, column_names_str));
    return executeQuery(select_rows_query);
}

ReadBufferPtr YTsaurusClient::selectRows(const String & cypress_path, const ColumnsWithTypeAndName & columns)
{
    String columns_names_str;
    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns_names_str += columns[i].name;
        if (i + 1 != columns.size())
            columns_names_str += ", ";
    }
    return selectRows(cypress_path, columns_names_str);
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
        try
        {
            if (connection_info.enable_heavy_proxy_redirection && query->isHeavyQuery())
                host_for_request = getHeavyProxyURI(host_for_request);

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
    std::string output_params = fmt::format("<uuid_mode=text_yql;complex_type_mode=positional;encode_utf8={}>", connection_info.encode_utf8  ?  "true" : "false");
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

    readStringUntilEOF(json_str, *buf);
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var json = parser.parse(json_str);
    return json;
}

Poco::JSON::Object::Ptr YTsaurusClient::getNodeMetadata(const String & cypress_path)
{
    String attributes_path = cypress_path + "/@";
    auto json = getMetadata(attributes_path);
    return json.extract<Poco::JSON::Object::Ptr>();
}

Poco::Dynamic::Var YTsaurusClient::getNodeAttribute(const String & cypress_path, const String & attribute_name)
{
    String attribute_path = cypress_path + "/@" + attribute_name;
    auto json = getMetadata(attribute_path);
    return json;
}

YTsaurusClient::SchemaDescription YTsaurusClient::getTableSchema(const String & cypress_path)
{
    auto schema = getNodeAttribute(cypress_path, "schema");
    const auto & schema_json = schema.extract<Poco::JSON::Object::Ptr>();

    if (!schema_json->has("$attributes"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "No \"$attributes\" property in yt table schema");

    auto attributes = schema_json->get("$attributes").extract<Poco::JSON::Object::Ptr>();
    if (!attributes->has("strict"))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Broken YtSaurus schema json. Missing `strict` field in attributes.");

    bool is_strict = attributes->getValue<bool>("strict");

    // Doesn't make sense to continue, schema isn't strict.
    if (!is_strict)
        return {is_strict, {}};

    if (!schema_json->has("$value"))
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "No \"$value\" property in yt table schema");
    }

    auto columns_array = schema_json->get("$value").extract<Poco::JSON::Array::Ptr>();
    std::unordered_map<String, DataTypePtr> yt_columns;

    for (const auto& yt_column : *columns_array) {
        const auto & yt_column_json = yt_column.extract<Poco::JSON::Object::Ptr>();
        if (!yt_column_json->has("name"))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Broken YtSaurus schema json. Missing `name` field.");

        auto yt_column_name = yt_column_json->getValue<String>("name");
        auto data_type = convertYTSchema(yt_column_json);

        yt_columns.insert({std::move(yt_column_name), data_type});
    }
    return {true, std::move(yt_columns)};
}

bool YTsaurusClient::checkSchemaCompatibility(const String & table_path, const SharedHeader & sample_block, String & reason, bool allow_nullable)
{
    auto yt_schema = getTableSchema(table_path);

    if (!yt_schema.is_strict)
        return true;

    for (const auto & column_type_with_name : sample_block->getColumnsWithTypeAndName())
    {
        auto iter = yt_schema.columns.find(column_type_with_name.name);
        if (iter == yt_schema.columns.end())
        {
            reason = fmt::format("There are no column with name {} in YtSaurus table", column_type_with_name.name);
            return false;
        }
        auto yt_column_type = iter->second;
        if (!isYTSaurusTypesCompatible(column_type_with_name.type, yt_column_type, allow_nullable))
        {
            reason = fmt::format("Column {} types mismatch. YtSaurus converted type {} table column type {}", column_type_with_name.name, yt_column_type->getName(), column_type_with_name.type->getName());
            return false;
        }
    }
    return true;
}

size_t YTsaurusClient::getTableNumberOfRows(const String & table_path)
{
    String lock_metadata_path = fmt::format("{}/@row_count", table_path);
    YTsaurusQueryPtr get_query(new YTsaurusGetQuery(lock_metadata_path));
    auto read_buff = executeQuery(get_query);
    size_t row_count;
    DB::readIntText(row_count, *read_buff);
    return row_count;
}

YTsaurusTableLock::YTsaurusTableLock(YTsaurusClientPtr client_, const String & cypress_path_, size_t transaction_timeout_ms)
    : client(client_)
{
    transaction_id = client->startTx(transaction_timeout_ms);
    lock_id = client->lock(cypress_path_, transaction_id);
    auto node_id = client->getNodeIdFromLock(lock_id);
    node_cypress_path = fmt::format("#{}",node_id);
}

YTsaurusTableLock::~YTsaurusTableLock()
{
    if (!transaction_id.empty())
    {
        try {
            client->commitTx(transaction_id);
        }
        catch (Exception & e)
        {
            LOG_WARNING(getLogger("YTsaurusTableLock"), "Can't commit transaction {}. Leave it. Exception: {}",
                transaction_id, e.displayText());
        }
    }
}

}
#endif
