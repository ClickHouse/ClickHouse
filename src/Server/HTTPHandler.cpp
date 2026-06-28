#include <Server/HTTPHandler.h>
#include <Server/HTTPQueryConstructor.h>

#include <Access/AccessControl.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Core/ExternalTable.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBufferFromString.h>
#include <Poco/URI.h>
#include <Common/quoteString.h>
#include <Disks/StoragePolicy.h>
#include <IO/CascadeWriteBuffer.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/TableNameHints.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Parsers/Lexer.h>
#include <Parsers/QueryParameterVisitor.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Session.h>
#include <Processors/Port.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTPHandlerRequestFilter.h>
#include <Server/IServer.h>
#include <Common/CurrentThread.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/maskSensitiveQueryParameters.h>
#include <Common/SettingsChanges.h>
#include <Common/StringUtils.h>
#include <Common/scope_guard_safe.h>
#include <Common/setThreadName.h>
#include <Common/typeid_cast.h>
#include <Parsers/ASTSetQuery.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <base/getFQDNOrHostName.h>
#include <base/isSharedPtrUnique.h>
#include <Server/HTTP/HTTPResponse.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/deferHTTP100Continue.h>
#include <Server/HTTP/sendExceptionToHTTPClient.h>
#include <Server/HTTP/setReadOnlyIfHTTPMethodIdempotent.h>

#include <Poco/Net/HTTPMessage.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <algorithm>
#include <boost/algorithm/string/trim.hpp>
#include <memory>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <utility>


namespace DB
{
namespace Setting
{
    extern const SettingsBool add_http_cors_header;
    extern const SettingsBool cancel_http_readonly_queries_on_client_close;
    extern const SettingsBool enable_http_compression;
    extern const SettingsUInt64 http_headers_progress_interval_ms;
    extern const SettingsUInt64 http_max_request_param_data_size;
    extern const SettingsBool http_native_compression_disable_checksumming_on_decompress;
    extern const SettingsUInt64 http_response_buffer_size;
    extern const SettingsBool http_wait_end_of_query;
    extern const SettingsBool http_write_exception_in_output_format;
    extern const SettingsInt64 http_zlib_compression_level;
    extern const SettingsUInt64 input_format_max_block_wait_ms;
    extern const SettingsUInt64 readonly;
    extern const SettingsBool send_progress_in_http_headers;
    extern const SettingsInt64 zstd_window_log_max;

    extern const SettingsBool http_allow_database_as_path;
    extern const SettingsBool http_allow_table_as_file;
    extern const SettingsBool http_allow_filters_as_path;
    extern const SettingsBool http_allow_filters_as_unrecognized_url_parameters;
    extern const SettingsString compression;
    extern const SettingsString filter;
    extern const SettingsString format;
    extern const SettingsString input_format;
    extern const SettingsString output_format;
    extern const SettingsString default_format;
    extern const SettingsString database;
    extern const SettingsString implicit_table_at_top_level;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;

    extern const int NO_ELEMENTS_IN_CONFIG;

    extern const int INVALID_SESSION_TIMEOUT;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int SESSION_ID_EMPTY;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}

namespace
{
bool tryAddHTTPOptionHeadersFromConfig(HTTPServerResponse & response, const Poco::Util::LayeredConfiguration & config)
{
    if (config.has("http_options_response"))
    {
        Strings config_keys;
        config.keys("http_options_response", config_keys);
        for (const std::string & config_key : config_keys)
        {
            if (config_key == "header" || config_key.starts_with("header["))
            {
                /// If there is empty header name, it will not be processed and message about it will be in logs
                if (config.getString("http_options_response." + config_key + ".name", "").empty())
                    LOG_WARNING(getLogger("processOptionsRequest"), "Empty header was found in config. It will not be processed.");
                else
                    response.add(config.getString("http_options_response." + config_key + ".name", ""),
                                 config.getString("http_options_response." + config_key + ".value", ""));

            }
        }
        return true;
    }
    return false;
}

/// Process options request. Useful for CORS.
void processOptionsRequest(HTTPServerResponse & response, const Poco::Util::LayeredConfiguration & config)
{
    /// If can add some headers from config
    if (tryAddHTTPOptionHeadersFromConfig(response, config))
    {
        response.setKeepAlive(false);
        response.setStatusAndReason(HTTPResponse::HTTP_NO_CONTENT);
        response.send();
    }
}
}

static std::chrono::steady_clock::duration parseSessionTimeout(
    const Poco::Util::AbstractConfiguration & config,
    const HTMLForm & params)
{
    unsigned session_timeout = config.getInt("default_session_timeout", 60);

    if (params.has("session_timeout"))
    {
        unsigned max_session_timeout = config.getUInt("max_session_timeout", 3600);
        std::string session_timeout_str = params.get("session_timeout");

        ReadBufferFromString buf(session_timeout_str);
        if (!tryReadIntText(session_timeout, buf) || !buf.eof())
            throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Invalid session timeout: '{}'", session_timeout_str);

        if (session_timeout > max_session_timeout)
            throw Exception(ErrorCodes::INVALID_SESSION_TIMEOUT, "Session timeout '{}' is larger than max_session_timeout: {}. "
                "Maximum session timeout could be modified in configuration file.",
                session_timeout_str, max_session_timeout);
    }

    return std::chrono::seconds(session_timeout);
}

/// Returns true if `url` starts with `prefix` and either matches it exactly or is followed by a
/// segment boundary (`/`, `?`, or `#`). Plain `starts_with` would also accept `/api/v11/...` under
/// prefix `/api/v1`, which leaks unrelated endpoints into the dynamic-query factory and produces
/// nonsensical path parsing after the prefix is stripped.
static bool hasUrlPrefixWithSegmentBoundary(std::string_view url, std::string_view prefix)
{
    if (!url.starts_with(prefix))
        return false;
    if (url.size() == prefix.size())
        return true;
    /// A prefix that already ends in a boundary character (e.g. `/api/v1/`) is itself
    /// segment-aligned, so any non-empty continuation is a valid child path: `/api/v1/db/hits`
    /// must match prefix `/api/v1/` even though `url[prefix.size()]` is `d`.
    if (!prefix.empty() && (prefix.back() == '/' || prefix.back() == '?' || prefix.back() == '#'))
        return true;
    const char next = url[prefix.size()];
    return next == '/' || next == '?' || next == '#';
}

HTTPHandlerConnectionConfig::HTTPHandlerConnectionConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (config.has(config_prefix + ".handler.password"))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "{}.handler.password will be ignored. Please remove it from config.", config_prefix);
    if (config.has(config_prefix + ".handler.user"))
        credentials.emplace(config.getString(config_prefix + ".handler.user", "default"));
}


HTTPHandler::HTTPHandler(IServer & server_, const HTTPHandlerConnectionConfig & connection_config_, const std::string & name, const HTTPResponseHeaderSetup & http_response_headers_override_, const std::string & url_prefix_, HTTPPathHintsPtr path_hints_)
    : log(getLogger(name))
    , server(server_)
    , default_settings(server.context()->getSettingsRef())
    , http_response_headers_override(http_response_headers_override_)
    , url_prefix(url_prefix_)
    , path_hints(std::move(path_hints_))
    , connection_config(connection_config_)
{
    server_display_name = server.config().getString("display_name", getFQDNOrHostName());
}


/// We need d-tor to be present in this translation unit to make it play well with some
/// forward decls in the header. Other than that, the default d-tor would be OK.
HTTPHandler::~HTTPHandler() = default;


bool HTTPHandler::authenticateUser(HTTPServerRequest & request, HTMLForm & params, HTTPServerResponse & response)
{
    return authenticateUserByHTTP(request, params, response, *session, request_credentials, connection_config, server.context(), log);
}


void HTTPHandler::processQuery(
    HTTPServerRequest & request,
    HTMLForm & params,
    HTTPServerResponse & response,
    Output & used_output,
    QueryScope & query_scope,
    const ProfileEvents::Event & write_event)
{
    using namespace Poco::Net;

    LOG_TRACE(log, "Request URI: {}", maskSensitiveQueryParametersInURI(request.getURI()));

    if (!authenticateUser(request, params, response))
        return; // '401 Unauthorized' response with 'Negotiate' has been sent at this point.

    /// The user could specify session identifier and session timeout.
    /// It allows to modify settings, create temporary tables and reuse them in subsequent requests.

    String session_id;
    std::chrono::steady_clock::duration session_timeout;
    bool session_is_set = params.has("session_id");
    const auto & config = server.config();

    /// Close http session (if any) after processing the request
    bool close_session = false;
    if (params.getParsed<bool>("close_session", false) && server.config().getBool("enable_http_close_session", true))
        close_session = true;

    if (session_is_set)
    {
        session_id = params.get("session_id");
        if (session_id.empty())
            throw Exception(ErrorCodes::SESSION_ID_EMPTY, "Session id query parameter was provided, but it was empty");
        session_timeout = parseSessionTimeout(config, params);
        std::string session_check = params.get("session_check", "");
        session->makeSessionContext(session_id, session_timeout, session_check == "1");
    }
    else
    {
        session_id = "";
        /// We should create it even if we don't have a session_id
        session->makeSessionContext();
    }

    /// We need to have both releasing/closing a session here and below. The problem with having it only as a SCOPE_EXIT
    /// is that it will be invoked after finalizing the buffer in the end of processQuery, and that technically means that
    /// the client has received all the data, but the session is not released yet. And it can (and sometimes does) happen
    /// that we'll try to acquire the same session in another request before releasing the session here, and the session for
    /// the following request will be technically locked, while it shouldn't be.
    /// Also, SCOPE_EXIT is still needed to release a session in case of any exception. If the exception occurs at some point
    /// after releasing the session below, this whole call will be no-op (due to named_session being nullptr already inside a session).
    SCOPE_EXIT_SAFE({ releaseOrCloseSession(session_id, close_session); });

    /// === Authentication and user profile are applied first ===
    /// Authentication has already happened above (line `authenticateUser`); makeQueryContext()
    /// loads the user's default profile. Auth-related parameters (role) are applied immediately
    /// after, so that the resulting settings/constraints are in effect before we process any
    /// general settings.
    auto context = session->makeQueryContext();

    auto roles = params.getAll("role");
    if (!roles.empty())
        context->setCurrentRoles(roles);

    /// Anything else beside HTTP POST should be readonly queries.
    setReadOnlyIfHTTPMethodIdempotent(context, request.getMethod());

    /// Set the query id supplied by the user, if any, and also update the OpenTelemetry fields.
    String query_id = params.get("query_id", request.get("X-ClickHouse-Query-Id", ""));

    /// Sanitize query_id: remove ASCII control characters to prevent CRLF injection
    /// into HTTP response headers (the query_id is reflected in X-ClickHouse-Query-Id).
    std::erase_if(query_id, [](unsigned char c) { return isControlASCII(c) || c == 0x7F; });

    context->setCurrentQueryId(query_id);

    bool has_external_data = startsWith(request.getContentType(), "multipart/form-data");

    auto param_could_be_skipped = [&] (const String & name)
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (name.empty())
            return true;

        /// HTTP-specific parameters that are consumed by the handler itself and never propagated as settings.
        /// `database` and `default_format` are NOT here — they are now proper settings and flow through the
        /// settings pipeline (with `changeable_in_readonly` constraints in the default profile so they remain
        /// settable on read-only HTTP methods).
        /// `filter` is NOT here either: it is collected as a construction filter only after
        /// `customizeQueryParam` has had a chance to bind it (e.g. a `predefined_query_handler` with a
        /// `{filter:String}` query parameter); see the parameter loop below.
        static const NameSet reserved_param_names{"compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace", "role",
            "buffer_size", "wait_end_of_query", "session_id", "session_timeout", "session_check", "client_protocol_version", "close_session"};

        if (reserved_param_names.contains(name))
            return true;

        if (has_external_data)
        {
            /// For external data we have unspecified parameters which literally are {'<temp_table_name>_format', '<temp_table_name>_types', '<temp_table_name>_structure'}.
            /// That parameters are not supposed to be used in the query as a settings. They have to be skipped.
            /// But we could not just skip all parameters with suffixes '_format', '_types', '_structure',
            /// because some of them are used in the query as a settings, like 'date_time_input_format',
            static const Names reserved_param_suffixes = {"_format", "_types", "_structure"};
            for (const String & suffix : reserved_param_suffixes)
            {
                if (endsWith(name, suffix))
                    return (!context->getAccessControl().isSettingNameAllowed(name));
            }
        }

        return false;
    };

    auto is_known_setting = [&](const String & name) -> bool
    {
        /// A few names aren't declared as settings via `DECLARE(...)` but are still handled by
        /// `Context::setSetting` as "settings" — most importantly `profile`, which triggers
        /// profile loading rather than mapping to a stored value. Without this carve-out, those
        /// names would be deferred to the unrecognized-URL-params path and (if the
        /// `http_allow_filters_as_unrecognized_url_parameters` feature is on) misinterpreted as
        /// filter expressions.
        if (name == "profile")
            return true;
        return context->getAccessControl().isSettingNameAllowed(name);
    };

    /// Collect filter URL parameters and unrecognized parameters (as filters when enabled).
    /// We need to consult the resolved settings to decide what to do with unrecognized params,
    /// so we apply settings in two phases: first the recognized ones, then optionally add filters
    /// from unrecognized ones.
    std::vector<String> url_filters_from_params;
    std::vector<std::pair<String, String>> deferred_unrecognized_params;

    /// Settings can be overridden in the query.
    SettingsChanges settings_changes;
    for (const auto & [key, value] : params)
    {
        if (param_could_be_skipped(key))
            continue;
        if (customizeQueryParam(context, key, value))
            continue;
        /// `filter` is a construction setting, but the HTTP interface allows multiple `?filter=`
        /// parameters combined with AND, so collect them here rather than letting the single-valued
        /// `is_known_setting` path apply only the last one. This runs after `customizeQueryParam`, so
        /// a configured handler that binds a `filter` query parameter (e.g. a `predefined_query_handler`
        /// with `{filter:String}`) still receives it.
        if (key == "filter")
        {
            url_filters_from_params.push_back("(" + value + ")");
            continue;
        }
        /// Recognized as a setting if it has a known setting name or starts with allowed prefixes.
        /// Otherwise, defer for possible filter treatment.
        if (is_known_setting(key))
            settings_changes.setSetting(key, value);
        else
            deferred_unrecognized_params.emplace_back(key, value);
    }

    /// `X-ClickHouse-Database` and `X-ClickHouse-Format` headers are aliases for the
    /// `database` and `default_format` settings. They override any matching URL parameter
    /// (preserving the historical precedence).
    if (auto header_value = request.get("X-ClickHouse-Database", ""); !header_value.empty())
        settings_changes.setSetting("database", header_value);
    if (auto header_value = request.get("X-ClickHouse-Format", ""); !header_value.empty())
        settings_changes.setSetting("default_format", header_value);

    context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    context->applySettingsChanges(settings_changes);

    const auto & settings = context->getSettingsRef();

    /// === URL path parsing happens after settings are applied ===
    /// This way the `http_allow_*_as_path` settings are read from the authenticated, profile-resolved
    /// context (the same way any per-user setting works), not from server defaults. If the user
    /// has all path features off, we skip path parsing entirely — the rest of the request is treated
    /// exactly as it would be at the root URL.
    HTTPPathInfo path_info;
    bool any_path_feature_enabled = settings[Setting::http_allow_database_as_path]
                                  || settings[Setting::http_allow_table_as_file]
                                  || settings[Setting::http_allow_filters_as_path];
    if (any_path_feature_enabled)
    {
        const String & raw_uri = request.getURI();
        String path_only = raw_uri;
        auto qmark = path_only.find('?');
        if (qmark != String::npos)
            path_only = path_only.substr(0, qmark);
        /// URL-decode the path.
        try
        {
            Poco::URI uri_obj(raw_uri);
            path_only = uri_obj.getPath();
        }
        catch (const Poco::Exception &) // NOLINT(bugprone-empty-catch)
        {
            /// Fall back to the already-stripped raw path.
        }

        /// If this handler is registered under a URL prefix, strip it so only the trailing portion
        /// is interpreted as `database/table.format` (or filters / hive partitions). Require a
        /// segment boundary so that prefix `/api/v1` does not also match `/api/v11/...`.
        if (!url_prefix.empty() && hasUrlPrefixWithSegmentBoundary(path_only, url_prefix))
        {
            path_only = path_only.substr(url_prefix.size());
            if (path_only.empty())
                path_only = "/";
        }

        path_info = parseHTTPPath(
            path_only,
            settings[Setting::http_allow_database_as_path],
            settings[Setting::http_allow_table_as_file],
            settings[Setting::http_allow_filters_as_path]);
    }

    /// Resolve the current database from the path and the `database` setting, in that order.
    /// If both are specified and differ, that's an error. We do this *before* `QueryScope::create`
    /// — if `setCurrentDatabase` throws (e.g. the database doesn't exist), the exception unwinds
    /// cleanly without leaving an in-flight query scope behind that would deadlock the response
    /// buffers.
    /// When the URL path supplied the database, we additionally append a hint about the closest
    /// matching configured HTTP handler path (e.g. `/dashboard`) so a user who typed `/sashbord`
    /// sees both the closest-handler suggestion and the closest-database-name suggestion.
    {
        /// The database explicitly supplied by *this request* via the `database` URL parameter or the
        /// `X-ClickHouse-Database` header — it was collected into `settings_changes` and already
        /// validated/applied above. A profile *default* for `database` is deliberately not treated as
        /// request-supplied, so a path like `/db2/table` does not spuriously conflict with (and is not
        /// blocked by) an inherited default — the path simply takes precedence over the default.
        const Field * explicit_database_field = settings_changes.tryGet("database");
        const String explicit_database = explicit_database_field ? explicit_database_field->safeGet<String>() : "";

        String resolved_database = settings[Setting::database];
        if (!path_info.database.empty())
        {
            if (!explicit_database.empty() && explicit_database != path_info.database)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Conflicting database specification: '{}' in URL path vs '{}' in `database` setting.",
                    path_info.database, explicit_database);

            /// A database from the URL path bypasses the `database` URL-parameter/header pipeline, so
            /// run it through `checkSettingsConstraints` here too — otherwise a `database` constraint
            /// (e.g. marked `const`, or restricted values) would protect the URL-parameter/header form
            /// but not the `/database/table` path form. The path takes precedence over a profile default.
            ///
            /// Apply it as the `database` setting as well (not just `setCurrentDatabase` below): `executeQuery`
            /// re-applies a non-empty `database` setting via `setCurrentDatabase` after the query SETTINGS are
            /// resolved, so an inherited profile default such as `database = 'db1'` would otherwise switch the
            /// query back to `db1` after `/db2/...` had already set the current database — making unqualified
            /// names resolve in the wrong database. Overwriting the setting here keeps the two in sync.
            SettingsChanges database_change;
            database_change.setSetting("database", path_info.database);
            context->checkSettingsConstraints(database_change, SettingSource::QUERY);
            context->applySettingsChanges(database_change);
            resolved_database = path_info.database;
        }
        if (!resolved_database.empty())
        {
            try
            {
                context->setCurrentDatabase(resolved_database);
            }
            catch (Exception & e)
            {
                if (!path_info.database.empty() && path_hints)
                {
                    auto handler_hints = path_hints->getHints("/" + path_info.database);
                    if (!handler_hints.empty())
                        e.addMessage("Or maybe HTTP handler {}?", handler_hints.front());
                }
                throw;
            }
        }
    }

    /// Initialize query scope, once query_id is initialized.
    /// (To track as much allocations as possible)
    query_scope = QueryScope::create(context);

    /// Now we know the resolved settings. Decide what to do with unrecognized URL params:
    /// - if http_allow_filters_as_unrecognized_url_parameters is true: treat them as filter expressions
    /// - otherwise: pass them through as settings (which will likely fail with "unknown setting"),
    ///   preserving the original behavior.
    std::vector<String> url_filters_from_unrecognized;
    if (settings[Setting::http_allow_filters_as_unrecognized_url_parameters])
    {
        for (const auto & [key, value] : deferred_unrecognized_params)
        {
            String f = parseURLParameterAsFilter(key, value);
            if (!f.empty())
                url_filters_from_unrecognized.push_back(f);
        }
    }
    else
    {
        SettingsChanges extra_changes;
        for (const auto & [key, value] : deferred_unrecognized_params)
            extra_changes.setSetting(key, value);
        if (!extra_changes.empty())
        {
            context->checkSettingsConstraints(extra_changes, SettingSource::QUERY);
            context->applySettingsChanges(extra_changes);
        }
    }

    /// This parameter is used to tune the behavior of output formats (such as Native) for compatibility.
    if (params.has("client_protocol_version"))
    {
        UInt64 version_param = parse<UInt64>(params.get("client_protocol_version"));
        context->setClientProtocolVersion(version_param);
    }

    /// Apply compression from path (if no `compression` setting was specified explicitly).
    SettingsChanges path_derived_changes;
    if (!path_info.compression.empty())
    {
        const String & current_compression = settings[Setting::compression];
        /// Compare the resolved `CompressionMethod`, not the raw strings: `chooseCompressionMethod`
        /// treats `gzip`/`gz`, `zstd`/`zst`, `lzma`/`xz`, `brotli`/`br` etc. as aliases, so
        /// `/hits.CSV.gz?compression=gzip` must not be rejected as a conflict.
        if (!current_compression.empty()
            && chooseCompressionMethod({}, current_compression) != chooseCompressionMethod({}, path_info.compression))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Conflicting compression: '{}' in URL path vs '{}' in `compression` setting.",
                path_info.compression, current_compression);
        if (current_compression.empty())
            path_derived_changes.setSetting("compression", path_info.compression);
    }
    /// Apply format from path (if no explicit `format`/`output_format`/`default_format` override exists).
    /// "When there is a format from the file extension and there is also an explicit override, the override wins."
    if (!path_info.format.empty())
    {
        const String & format_override = settings[Setting::format];
        const String & output_format_override = settings[Setting::output_format];
        const String & default_format_setting = settings[Setting::default_format];
        if (format_override.empty() && output_format_override.empty() && default_format_setting.empty())
        {
            /// Use as default_format so that queries without explicit FORMAT honor it.
            path_derived_changes.setSetting("default_format", path_info.format);
        }
    }
    if (!path_derived_changes.empty())
    {
        context->checkSettingsConstraints(path_derived_changes, SettingSource::QUERY);
        context->applySettingsChanges(path_derived_changes);
    }

    /// The client can pass a HTTP header indicating supported compression method (gzip or deflate).
    String http_response_compression_methods = request.get("Accept-Encoding", "");
    CompressionMethod http_response_compression_method = CompressionMethod::None;

    if (!http_response_compression_methods.empty())
        http_response_compression_method = chooseHTTPCompressionMethod(http_response_compression_methods);

    bool client_supports_http_compression = http_response_compression_method != CompressionMethod::None;

    /// Client can pass a 'compress' flag in the query string. In this case the query result is
    /// compressed using internal algorithm. This is not reflected in HTTP headers.
    bool internal_compression = params.getParsedLast<bool>("compress", false);

    /// If wait_end_of_query is specified, the whole result will be buffered.
    ///  First ~buffer_size bytes will be buffered in memory, the remaining bytes will be stored in temporary file.
    auto buffer_size_http = settings[Setting::http_response_buffer_size];
    /// setting overrides deprecated buffer_size parameter
    if (!params.has("http_response_buffer_size"))
        buffer_size_http = params.getParsedLast<size_t>("buffer_size", buffer_size_http);

    bool wait_end_of_query = settings[Setting::http_wait_end_of_query];
    /// setting overrides deprecated wait_end_of_query parameter
    if (!params.has("http_wait_end_of_query"))
        wait_end_of_query = params.getParsedLast<bool>("wait_end_of_query", wait_end_of_query);

    bool enable_http_compression = params.getParsedLast<bool>("enable_http_compression", settings[Setting::enable_http_compression]);
    Int64 http_zlib_compression_level
        = params.getParsed<Int64>("http_zlib_compression_level", settings[Setting::http_zlib_compression_level]);

    used_output.out_holder =
        std::make_shared<WriteBufferFromHTTPServerResponse>(
            response,
            request.getMethod() == HTTPRequest::HTTP_HEAD,
            write_event);
    used_output.out_maybe_compressed = used_output.out_holder;
    used_output.out = used_output.out_holder;

    if (client_supports_http_compression && enable_http_compression)
    {
        used_output.out_holder->setCompressionMethodHeader(http_response_compression_method);
        used_output.wrap_compressed_holder = wrapWriteBufferWithCompressionMethod(
            used_output.out.get(),
            http_response_compression_method,
            static_cast<int>(http_zlib_compression_level),
            0,
            DBMS_DEFAULT_BUFFER_SIZE,
            nullptr,
            0,
            false);
        used_output.out_maybe_compressed = used_output.wrap_compressed_holder;
        used_output.out = used_output.wrap_compressed_holder;
    }

    /// Generic response-body compression from the `compression` setting (or URL path file extension).
    /// This is independent of HTTP Content-Encoding — the bytes sent to the client are compressed
    /// and the client is expected to decompress them.
    const String & response_compression_name = settings[Setting::compression];
    if (!response_compression_name.empty())
    {
        CompressionMethod response_compression_method = chooseCompressionMethod({}, response_compression_name);
        if (response_compression_method == CompressionMethod::None)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unknown compression method: '{}' in `compression` setting.", response_compression_name);
        used_output.generic_compression_holder = wrapWriteBufferWithCompressionMethod(
            used_output.out.get(),
            response_compression_method,
            static_cast<int>(http_zlib_compression_level),
            0,
            DBMS_DEFAULT_BUFFER_SIZE,
            nullptr,
            0,
            false);
        used_output.out_maybe_compressed = used_output.generic_compression_holder;
        used_output.out = used_output.generic_compression_holder;
    }

    if (internal_compression)
    {
        used_output.out_compressed_holder = std::make_shared<CompressedWriteBuffer>(*used_output.out);
        used_output.out_maybe_compressed = used_output.out_compressed_holder;
        used_output.out = used_output.out_compressed_holder;
    }

    if (buffer_size_http > 0 || wait_end_of_query)
    {
        CascadeWriteBuffer::WriteBufferPtrs cascade_buffers;
        CascadeWriteBuffer::WriteBufferConstructors cascade_buffers_lazy;

        if (buffer_size_http > 0)
            cascade_buffers.emplace_back(std::make_shared<MemoryWriteBuffer>(buffer_size_http));

        if (wait_end_of_query)
        {
            auto tmp_data = server.context()->getTempDataOnDisk();
            cascade_buffers_lazy.emplace_back([tmp_data](const WriteBufferPtr &) -> WriteBufferPtr
            {
                return std::make_unique<TemporaryDataBuffer>(tmp_data);
            });
        }
        else
        {
            auto push_memory_buffer_and_continue = [next_buffer = used_output.out] (const WriteBufferPtr & prev_buf)
            {
                auto * prev_memory_buffer = typeid_cast<MemoryWriteBuffer *>(prev_buf.get());
                if (!prev_memory_buffer)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected MemoryWriteBuffer");

                auto rdbuf = prev_memory_buffer->tryGetReadBuffer();
                copyData(*rdbuf, *next_buffer);

                return next_buffer;
            };

            cascade_buffers_lazy.emplace_back(push_memory_buffer_and_continue);
        }

        used_output.out_delayed_and_compressed_holder = std::make_unique<CascadeWriteBuffer>(std::move(cascade_buffers), std::move(cascade_buffers_lazy));
        used_output.out_maybe_delayed_and_compressed = used_output.out_delayed_and_compressed_holder;
    }
    else
    {
        used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed;
    }

    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    String http_request_compression_method_str = request.get("Content-Encoding", "");
    int zstd_window_log_max = static_cast<int>(context->getSettingsRef()[Setting::zstd_window_log_max]);
    /// TODO check
    /// input stream are hold inside in_post instance
    auto in_post = wrapReadBufferWithCompressionMethod(
        wrapReadBufferPointer(request.getStream()),
        chooseCompressionMethod({}, http_request_compression_method_str),
        zstd_window_log_max);
    LOG_DEBUG(getLogger("HTTPServerRequest"), "creating in_post id {}", size_t(in_post.get()));


    /// The data can also be compressed using incompatible internal algorithm. This is indicated by
    /// 'decompress' query parameter.
    std::unique_ptr<ReadBuffer> in_post_maybe_compressed;
    bool is_in_post_compressed = false;
    if (params.getParsedLast<bool>("decompress", false))
    {
        in_post_maybe_compressed = std::make_unique<CompressedReadBuffer>(std::move(in_post), /* allow_different_codecs_ = */ false, /* external_data_ = */ true);
        is_in_post_compressed = true;
    }
    else
    {
        in_post_maybe_compressed = std::move(in_post);
    }

    /// NOTE: this may create pretty huge allocations that will not be accounted in trace_log,
    /// because memory_profiler_sample_probability/memory_profiler_step are not applied yet,
    /// they will be applied in ProcessList::insert() from executeQuery() itself.
    const auto & raw_query = getQuery(request, params, context);

    /// Combine all HTTP filter sources into the `filter` setting. The query-construction settings
    /// (`select`/`filter`/`order`/`sort`/`page`) are applied by the engine in `executeQuery`, on the
    /// parsed AST — see `applyQueryConstructionSettings`. The HTTP interface only needs to assemble
    /// the `filter` value here, because it has additional sources the engine does not: the existing
    /// `filter` setting value, filters from the URL path, repeated `?filter=` parameters, and (when
    /// enabled) unrecognized URL parameters, combined with `AND` in that order. `select`, `order`,
    /// `sort` and `page` are already regular settings applied from the URL parameters, so the engine
    /// reads them directly.
    {
        /// The filters that the HTTP request *adds* on top of the existing `filter` setting: filters
        /// from the URL path, repeated `?filter=` parameters, and (when enabled) unrecognized URL
        /// parameters.
        std::vector<String> added_filters;
        for (const auto & f : path_info.path_filters)
            added_filters.push_back(f);
        for (const auto & f : url_filters_from_params)
            added_filters.push_back(f);
        for (const auto & f : url_filters_from_unrecognized)
            added_filters.push_back(f);

        /// Only act when the request actually adds filters. Otherwise the existing `filter` setting
        /// (e.g. a profile default) is left untouched and applied as-is by
        /// `applyQueryConstructionSettings`. (Re-submitting the already-applied value through
        /// `checkSettingsConstraints` would reject a `filter` constrained as `const` even when the
        /// request changes nothing.)
        if (!added_filters.empty())
        {
            String added_combined;
            for (const auto & f : added_filters)
            {
                if (!added_combined.empty())
                    added_combined += " AND ";
                added_combined += f;
            }

            /// Enforce the `filter` constraint on the added sources: build the full value (the
            /// existing `filter` setting AND the added filters) and run `checkSettingsConstraints`,
            /// so a profile that constrains `filter` (e.g. marks it `const`) blocks adding filters
            /// via `?filter=` / the URL path. The check throws on violation; the value is not applied.
            std::vector<String> all_filters;
            if (!settings[Setting::filter].value.empty())
                all_filters.push_back("(" + settings[Setting::filter].value + ")");
            for (const auto & f : added_filters)
                all_filters.push_back(f);
            String full_filter;
            for (const auto & f : all_filters)
            {
                if (!full_filter.empty())
                    full_filter += " AND ";
                full_filter += f;
            }
            SettingsChanges filter_change;
            filter_change.setSetting("filter", full_filter);
            context->checkSettingsConstraints(filter_change, SettingSource::QUERY);

            /// Stash the added filters in an overwrite-immune context channel (NOT the `filter`
            /// setting) so they still apply — combined with `AND` — when the query carries its own
            /// `SETTINGS filter = ...` clause, which would otherwise overwrite the `filter` setting.
            context->setHTTPCombinedFilter(added_combined);
        }
    }

    /// Determine base query: from URL path table, or from `query` param (raw_query).
    String final_query = raw_query;

    if (!path_info.table.empty())
    {
        /// `qualified_table` is the back-quoted `database.table` (or just `table`) from the URL path.
        /// It is used both as SQL text spliced into a generated query (`SELECT * FROM <qualified_table>`)
        /// and as the value of the `implicit_table_at_top_level` setting for a FROM-less query. The
        /// analyzer parses that setting as a quoted compound identifier (see
        /// `QueryTreeBuilder::buildJoinTree`), so back-quoting is required and correct: it lets a
        /// database/table whose name needs quoting (e.g. `/weird-db/my-table`) and — crucially — a table
        /// name that contains a literal dot (e.g. `/db/my.table` → `` `db`.`my.table` ``) resolve to the
        /// right identifier parts instead of being split on every `.`.
        String qualified_table;
        if (!path_info.database.empty())
            qualified_table = backQuoteIfNeed(path_info.database) + "." + backQuoteIfNeed(path_info.table);
        else
            qualified_table = backQuoteIfNeed(path_info.table);

        /// Validate up-front that the table from the URL path actually exists, so a typo like
        /// `/sashboards` produces a single clean response that combines the closest table-name
        /// hint with the closest configured-handler hint (e.g. `/dashboard`). Without this check
        /// the table-existence error would be raised later from query execution and would carry
        /// only the table-name hint.
        ///
        /// Skip the pre-check when the parsed table name still contains a dot: that means the
        /// path parser tried to extract a format/compression extension and failed (e.g.
        /// `/hits.Parquet` on a build that does not register Parquet). Deferring to the normal
        /// query execution path preserves the existing response headers (Content-Disposition,
        /// X-ClickHouse-Format) that other tests assert on.
        ///
        /// Skip the pre-check when the user supplied an explicit query: either via the `query` URL
        /// parameter (`raw_query`) or via a `POST`/`PUT` body. In those cases the path table is at
        /// most a filename hint for `Content-Disposition`, or the source for
        /// `implicit_table_at_top_level` (only applied to FROM-less queries). Forcing the path
        /// table to exist would reject valid requests like `/foo.CSV?query=SELECT+1+FROM+other`
        /// or `POST /db/path_table` with body `SELECT ... FROM other_table`.
        bool request_has_body = (request.getMethod() == HTTPRequest::HTTP_POST
                                 || request.getMethod() == HTTPRequest::HTTP_PUT)
                              && (request.getChunkedTransferEncoding() || request.getContentLength64() > 0);
        const String table_db = path_info.database.empty() ? context->getCurrentDatabase() : path_info.database;
        bool table_name_is_simple = path_info.table.find('.') == String::npos;
        if (table_name_is_simple && !table_db.empty() && raw_query.empty() && !request_has_body)
        {
            StorageID table_id(table_db, path_info.table);
            if (!DatabaseCatalog::instance().isTableExist(table_id, context))
            {
                auto db_ptr = DatabaseCatalog::instance().tryGetDatabase(table_db);
                TableNameHints table_hints(db_ptr, context);
                auto table_name_hints = table_hints.getHints(path_info.table);
                /// Format the message so it remains greppable by historical tests that look for
                /// `There is no handle /X. Maybe you meant /Y` (02842, 03522). The leading clause
                /// matches `NotFoundHandler` exactly; the trailing clauses add the database/table
                /// context that the original handler did not have.
                String message = fmt::format("There is no handle /{}.", path_info.table);
                if (path_hints)
                {
                    auto handler_hints = path_hints->getHints("/" + path_info.table);
                    if (!handler_hints.empty())
                        message += fmt::format(" Maybe you meant {}?", handler_hints.front());
                }
                message += fmt::format(" Or table {} (which does not exist)", table_id.getNameForLogs());
                if (!table_name_hints.empty())
                    message += fmt::format(" - maybe you meant table {}", backQuoteIfNeed(table_name_hints.front()));
                message += ".";
                throw Exception(ErrorCodes::UNKNOWN_TABLE, "{}", message);
            }
        }

        /// Always set implicit_table_at_top_level so a FROM-less SELECT (whether user-supplied
        /// or auto-generated) picks up the table from the URL path.
        SettingsChanges implicit_change;
        implicit_change.setSetting("implicit_table_at_top_level", qualified_table);
        context->checkSettingsConstraints(implicit_change, SettingSource::QUERY);
        context->applySettingsChanges(implicit_change);

        /// If there is no user-supplied query (URL param empty AND no body), generate a default one.
        if (raw_query.empty() && !request_has_body)
            final_query = "SELECT * FROM " + qualified_table;
    }

    /// The query-construction settings (`select`/`filter`/`order`/`sort`/`page`) are applied by the
    /// engine (`executeQuery`) on the parsed AST, so the query text is not wrapped here. When the
    /// SQL comes from the request body, `final_query` is empty and the body is concatenated below;
    /// the engine then wraps the parsed body query just the same.
    const String & query = final_query;
    std::unique_ptr<ReadBuffer> in_param = std::make_unique<ReadBufferFromString>(query);

    used_output.out_holder->setSendProgress(settings[Setting::send_progress_in_http_headers]);
    used_output.out_holder->setSendProgressInterval(settings[Setting::http_headers_progress_interval_ms]);

    /// If 'http_native_compression_disable_checksumming_on_decompress' setting is turned on,
    /// checksums of client data compressed with internal algorithm are not checked.
    if (is_in_post_compressed && settings[Setting::http_native_compression_disable_checksumming_on_decompress])
        static_cast<CompressedReadBuffer &>(*in_post_maybe_compressed).disableChecksumming();

    /// Add CORS header if 'add_http_cors_header' setting is turned on send * in Access-Control-Allow-Origin
    /// Note that whether the header is added is determined by the settings, and we can only get the user settings after authentication.
    /// Once the authentication fails, the header can't be added.
    if (settings[Setting::add_http_cors_header] && !request.get("Origin", "").empty() && !config.has("http_options_response"))
        used_output.out_holder->addHeaderCORS(true);

    auto append_callback = [my_context = context] (ProgressCallback callback)
    {
        auto prev = my_context->getProgressCallback();

        my_context->setProgressCallback([prev, callback] (const Progress & progress)
        {
            if (prev)
                prev(progress);

            callback(progress);
        });
    };

    /// While still no data has been sent, we will report about query execution progress by sending HTTP headers.
    /// Note that we add it unconditionally so the progress is available for `X-ClickHouse-Summary`
    append_callback([&used_output, &context](const Progress & progress)
    {
        used_output.out_holder->onProgress(progress, context);
    });

    if (settings[Setting::readonly] > 0 && settings[Setting::cancel_http_readonly_queries_on_client_close])
    {
        append_callback([&context, &request](const Progress &)
        {
            /// Assume that at the point this method is called no one is reading data from the socket any more:
            /// should be true for read-only queries.
            if (!request.checkPeerConnected())
                context->killCurrentQuery();
        });
    }

    customizeContext(request, context, *in_post_maybe_compressed);
    std::unique_ptr<ReadBuffer> in;
    if (has_external_data)
    {
        in = std::move(in_param);
        in_post_maybe_compressed.reset();
    }
    else
    {
        in = std::make_unique<ConcatReadBuffer>(std::move(in_param), std::move(in_post_maybe_compressed));
    }

    applyHTTPResponseHeaders(response, http_response_headers_override);

    /// Capture data needed for Content-Disposition computation from the surrounding scope. The
    /// filename normally comes from the URL path component verbatim (preserving the case the user
    /// wrote, e.g. `hits.CSV.gz`); when the path did not name a table it is empty and a
    /// `result.<format>.<compression>` fallback is built below.
    ///
    /// An explicit output-format override (`output_format` / `format`) changes the bytes, so the
    /// path's format extension would be wrong (e.g. `/hits.CSV?output_format=Native` sends `Native`).
    /// In that case rebuild the filename from the path table name plus the override format (which
    /// carries the user's casing — the path-derived format goes to `default_format`, not these). The
    /// `compression` setting is handled similarly: if the response is compressed but the path
    /// filename does not already carry that extension (`/hits.CSV?compression=gz`), it is appended.
    String disposition_filename = path_info.filename_for_disposition;
    String disposition_base = path_info.table;
    String disposition_path_format = path_info.format;
    String disposition_compression = settings[Setting::compression];
    /// Canonicalize the compression to its file-extension form (`gzip` -> `gz`, `zstd` -> `zst`, …) so
    /// the `Content-Disposition` filename uses the same suffix as the URL path. Otherwise an accepted
    /// alias would duplicate the extension: `/hits.CSV.gz?compression=gzip` has a path filename of
    /// `hits.CSV.gz` but a raw `gzip` setting, and appending `.gzip` would yield `hits.CSV.gz.gzip`.
    /// Unknown values are left as-is (the response-buffer setup throws on them anyway).
    if (!disposition_compression.empty())
        if (String canonical = canonicalizeCompressionExtension(disposition_compression); !canonical.empty())
            disposition_compression = std::move(canonical);
    String disposition_format_override = !settings[Setting::output_format].value.empty()
        ? settings[Setting::output_format].value
        : settings[Setting::format].value;
    auto set_query_result
        = [&response, this, disposition_filename, disposition_base, disposition_path_format, disposition_compression, disposition_format_override]
        (const QueryResultDetails & details)
    {
        response.add("X-ClickHouse-Query-Id", details.query_id);

        if (!(http_response_headers_override && http_response_headers_override->contains(Poco::Net::HTTPMessage::CONTENT_TYPE))
            && details.content_type)
            response.setContentType(*details.content_type);

        if (details.format)
            response.add("X-ClickHouse-Format", *details.format);

        if (details.timezone)
            response.add("X-ClickHouse-Timezone", *details.timezone);

        if (details.query_cache_entry_created_at)
            response.add("Age", std::to_string(std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - *details.query_cache_entry_created_at).count()));

        if (details.query_cache_entry_expires_at)
            response.add("Expires", std::format("{:%a, %d %b %Y %H:%M:%S} GMT", *details.query_cache_entry_expires_at));

        /// Set Content-Disposition: attachment for binary/compressed responses.
        bool response_is_binary = details.format && isBinaryOutputFormat(*details.format);
        bool response_is_compressed = !disposition_compression.empty();
        if (response_is_binary || response_is_compressed)
        {
            String filename;
            if (!disposition_format_override.empty())
            {
                /// An explicit `output_format` / `format` override supersedes the path's format
                /// extension; rebuild from the path table name (or `result`) + the override format.
                filename = (disposition_base.empty() ? "result" : disposition_base) + "." + disposition_format_override;
                if (response_is_compressed)
                    filename += "." + disposition_compression;
            }
            else if (!disposition_filename.empty()
                && (disposition_path_format.empty() || !details.format || *details.format == disposition_path_format))
            {
                /// The path filename already reflects the effective output format (both `path_info.format`
                /// and `details.format` are canonical format names), so keep it — it preserves the user's
                /// casing (`/hits.csv`).
                filename = disposition_filename;
                /// The response is compressed (e.g. via `?compression=gz`); make sure the path-derived
                /// filename ends with the effective compression extension instead of advertising bytes
                /// it does not carry.
                if (response_is_compressed)
                {
                    const String compression_suffix = "." + disposition_compression;
                    if (!filename.ends_with(compression_suffix))
                        filename += compression_suffix;
                }
            }
            else if (details.format)
            {
                /// The effective output format (`details.format`) differs from the path's extension —
                /// this happens when a query-level `SETTINGS output_format = …` / `format = …` (applied
                /// inside `executeQuery`, after `disposition_format_override` was captured from the
                /// URL/profile settings) overrides the path. Honor the same "explicit override wins over
                /// the path extension" contract by rebuilding the filename from the path table name (or
                /// `result`) + the effective format.
                filename = (disposition_base.empty() ? "result" : disposition_base) + "." + *details.format;
                if (response_is_compressed)
                    filename += "." + disposition_compression;
            }
            else
            {
                /// No filename from the URL path: build `result.<format>[.<compression>]`.
                filename = "result";
                if (details.format)
                {
                    filename += ".";
                    filename += *details.format;
                }
                if (response_is_compressed)
                {
                    filename += ".";
                    filename += disposition_compression;
                }
            }
            /// Sanitize filename: strip control and quote characters to avoid header injection.
            String safe;
            safe.reserve(filename.size());
            for (char c : filename)
            {
                if (isControlASCII(static_cast<unsigned char>(c)) || c == '"' || c == '\\')
                    continue;
                safe += c;
            }
            response.set("Content-Disposition", "attachment; filename=\"" + safe + "\"");
        }

        for (const auto & [name, value] : details.additional_headers)
            response.set(name, value);
    };

    auto handle_exception_in_output_format = [&, session_id, close_session](IOutputFormat & current_output_format,
                                                 const String & format_name,
                                                 const ContextPtr & context_,
                                                 const std::optional<FormatSettings> & format_settings)
    {
        if (settings[Setting::http_write_exception_in_output_format] && current_output_format.supportsWritingException())
        {
            /// If wait_end_of_query=true in case of an exception all data written to output format during query execution will be
            /// ignored, so we cannot write exception message in current output format as it will be also ignored.
            /// Instead, we create exception_writer function that will write exception in required format
            /// and will use it later in trySendExceptionToClient when all buffers will be prepared.
            if (wait_end_of_query)
            {
                auto header = current_output_format.getPort(IOutputFormat::PortKind::Main).getHeader();
                used_output.exception_writer = [&, format_name, header, context_, format_settings, session_id, close_session](WriteBuffer & buf, int code, const String & message)
                {
                    if (used_output.out_holder->isCanceled())
                    {
                        chassert(buf.isCanceled());
                        return;
                    }

                    drainRequestIfNeeded(request, response);
                    used_output.out_holder->setExceptionCode(code);

                    auto output_format = FormatFactory::instance().getOutputFormat(format_name, buf, header, context_, format_settings);
                    output_format->setException(message);
                    output_format->finalize();
                    releaseOrCloseSession(session_id, close_session);
                    used_output.finalize();
                    used_output.exception_is_written = true;
                };
            }
            else
            {
                if (used_output.out_holder->isCanceled())
                    return;

                bool with_stacktrace = (params.getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true));
                ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace);

                drainRequestIfNeeded(request, response);
                used_output.out_holder->setExceptionCode(status.code);
                current_output_format.setException(status.message);
                current_output_format.finalize();
                releaseOrCloseSession(session_id, close_session);
                used_output.finalize();

                used_output.exception_is_written = true;
            }
        }
    };

    auto query_finish_callback = [&]()
    {
        releaseOrCloseSession(session_id, close_session);

        /// Flush all the data from one buffer to another, to track
        /// NetworkSendElapsedMicroseconds/NetworkSendBytes from the query
        /// context
        used_output.finalize();
    };

    /// Create callback to defer HTTP 100 Continue response to after quota checks
    HTTPContinueCallback http_continue_callback = {};
    if (shouldDeferHTTP100Continue(request))
    {
        http_continue_callback = [&request, &response]()
        {
            if (request.getExpectContinue() && response.getStatus() == HTTPResponse::HTTP_OK)
            {
                response.sendContinue();
            }
        };
    }

    QueryFlags query_flags;
    /// Streaming `INSERT` needs the parser to stop at the end of the URL-provided query so the
    /// request body stays intact for the input format. Only enable this when the URL query
    /// alone already begins with an `INSERT` statement, otherwise we would break the legacy
    /// behavior of splitting SQL text between the `query` parameter and the request body.
    /// Leading whitespace and SQL comments are skipped so that forms like `/*trace*/ INSERT ...`
    /// also take the streaming-safe parse path.
    auto url_query_starts_with_insert = [&query]()
    {
        Lexer lexer(query.data(), query.data() + query.size());
        Token token = lexer.nextToken();
        while (!token.isSignificant() && !token.isEnd() && !token.isError())
            token = lexer.nextToken();
        if (token.type != TokenType::BareWord)
            return false;
        static constexpr std::string_view kw = "INSERT";
        if (static_cast<size_t>(token.end - token.begin) != kw.size())
            return false;
        for (size_t j = 0; j < kw.size(); ++j)
            if (toUpperIfAlphaASCII(token.begin[j]) != kw[j])
                return false;
        return true;
    };
    query_flags.parse_query_from_initial_buffer
        = settings[Setting::input_format_max_block_wait_ms] != 0 && url_query_starts_with_insert();

    executeQuery(
        std::move(in),
        *used_output.out_maybe_delayed_and_compressed,
        context,
        set_query_result,
        query_flags,
        {},
        handle_exception_in_output_format,
        query_finish_callback,
        http_continue_callback);
}

bool HTTPHandler::trySendExceptionToClient(
    int exception_code, const std::string & message, HTTPServerRequest & request, HTTPServerResponse & response, Output & used_output)
try
{
    if (!used_output.out_holder && !used_output.exception_is_written)
    {
        /// If nothing was sent yet and we don't even know if we must compress the response.
        auto wb = WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD);
        return wb.cancelWithException(request, exception_code, message, nullptr);
    }

    chassert(used_output.out_maybe_compressed);

    /// Destroy CascadeBuffer to actualize buffers' positions and reset extra references
    if (used_output.hasDelayed())
    {
        /// do not call finalize here for CascadeWriteBuffer used_output.out_maybe_delayed_and_compressed,
        /// exception is written into used_output.out_maybe_compressed later
        auto write_buffers = used_output.out_delayed_and_compressed_holder->getResultBuffers();
        /// cancel the rest unused buffers
        for (auto & wb : write_buffers)
            if (isSharedPtrUnique(wb))
                wb->cancel();

        used_output.out_maybe_delayed_and_compressed = used_output.out_maybe_compressed;
        used_output.out_delayed_and_compressed_holder.reset();
    }

    if (used_output.exception_is_written)
    {
        LOG_TRACE(log, "Exception has already been written to output format by current output formatter, nothing to do");
        /// everything has been held by output format write
        return true;
    }

    /// We might have special formatter for exception message.
    if (used_output.exception_writer)
    {
        used_output.exception_writer(*used_output.out_maybe_compressed, exception_code, message);
        return true;
    }

    /// This is the worst case scenario
    /// There is no support from output_format.
    /// output_format either did not faced the exception or did not support the exception writing

    /// Send the error message into already used (and possibly compressed) stream.
    /// Note that the error message will possibly be sent after some data.
    /// Also HTTP code 200 could have already been sent.
    return used_output.out_holder->cancelWithException(request, exception_code, message, used_output.out_maybe_compressed.get());
}
catch (...)
{
    /// The message could be not sent due to error on allocations
    /// or due to exception in used_output.exception_writer
    /// or because the socket is broken
    tryLogCurrentException(log, "Cannot send exception to client");
    return false;
}

void HTTPHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event)
{
    DB::setThreadName(ThreadName::HTTP_HANDLER);

    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::HTTP, request.isSecure());
    SCOPE_EXIT({ session.reset(); });
    QueryScope query_scope;

    Output used_output;

    /// In case of exception, send stack trace to client.
    bool with_stacktrace = false;

    OpenTelemetry::TracingContextHolderPtr thread_trace_context;
    SCOPE_EXIT({
        // make sure the response status is recorded
        if (thread_trace_context)
            thread_trace_context->root_span.addAttribute("clickhouse.http_status", response.getStatus());
    });

    try
    {
        if (request.getMethod() == HTTPServerRequest::HTTP_OPTIONS)
        {
            processOptionsRequest(response, server.config());
            return;
        }

        // Parse the OpenTelemetry traceparent header.
        auto & client_trace_context = session->getClientTraceContext();
        if (request.has("traceparent"))
        {
            std::string opentelemetry_traceparent = request.get("traceparent");
            std::string error;
            if (!client_trace_context.parseTraceparentHeader(opentelemetry_traceparent, error))
            {
                LOG_DEBUG(log, "Failed to parse OpenTelemetry traceparent header '{}': {}", opentelemetry_traceparent, error);
            }
            client_trace_context.tracestate = request.get("tracestate", "");
        }

        // Setup tracing context for this thread
        auto context = session->sessionOrGlobalContext();
        thread_trace_context = std::make_unique<OpenTelemetry::TracingContextHolder>("HTTPHandler",
            client_trace_context,
            context->getSettingsRef(),
            context->getOpenTelemetrySpanLog());
        thread_trace_context->root_span.kind = OpenTelemetry::SpanKind::SERVER;
        thread_trace_context->root_span.addAttribute(
            "clickhouse.uri", [&] { return maskSensitiveQueryParametersInURI(request.getURI()); });
        thread_trace_context->root_span.addAttribute("http.referer", request.get("Referer", ""));
        thread_trace_context->root_span.addAttribute("http.user.agent", request.get("User-Agent", ""));
        thread_trace_context->root_span.addAttribute("http.method", request.getMethod());

        response.setContentType("text/plain; charset=UTF-8");
        response.add("Access-Control-Expose-Headers", "X-ClickHouse-Query-Id,X-ClickHouse-Summary,X-ClickHouse-Server-Display-Name,X-ClickHouse-Format,X-ClickHouse-Timezone,X-ClickHouse-Exception-Code,X-ClickHouse-Exception-Tag");
        response.set("X-ClickHouse-Server-Display-Name", server_display_name);

        if (!request.get("Origin", "").empty())
            tryAddHTTPOptionHeadersFromConfig(response, server.config());

        /// For keep-alive to work.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        HTMLForm params(default_settings, request);

        if (params.getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true))
            with_stacktrace = true;

        /// FIXME: maybe this check is already unnecessary.
        /// Workaround. Poco does not detect 411 Length Required case.
        if (request.getMethod() == HTTPRequest::HTTP_POST && !request.getChunkedTransferEncoding() && !request.hasContentLength())
        {
            throw Exception(ErrorCodes::HTTP_LENGTH_REQUIRED,
                            "The Transfer-Encoding is not chunked and there "
                            "is no Content-Length header for POST request");
        }

        processQuery(request, params, response, used_output, query_scope, write_event);
        if (request_credentials)
            LOG_DEBUG(log, "Authentication in progress...");
        else
            LOG_DEBUG(log, "Done processing query");
    }
    catch (...)
    {
        SCOPE_EXIT({
            request_credentials.reset(); // ...so that the next requests on the connection have to always start afresh in case of exceptions.
        });

        tryLogCurrentException(log);

        /** If exception is received from remote server, then stack trace is embedded in message.
          * If exception is thrown on local server, then stack trace is in separate field.
          */
        const bool include_version = session && session->sessionContext();
        ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace, include_version);
        auto error_sent = trySendExceptionToClient(status.code, status.message, request, response, used_output);

        used_output.cancel();

        if (!error_sent && thread_trace_context)
                thread_trace_context->root_span.addAttribute("clickhouse.exception", "Cannot flush data to client");

        if (thread_trace_context)
            thread_trace_context->root_span.addAttribute(status);
    }
}

void HTTPHandler::releaseOrCloseSession(const String & session_id, bool close_session)
{
    if (!session_id.empty())
    {
        if (close_session)
            session->closeSession(session_id);
        else
            session->releaseSessionID();
    }
}

DynamicQueryHandler::DynamicQueryHandler(
    IServer & server_,
    const HTTPHandlerConnectionConfig & connection_config_,
    const std::string & param_name_,
    const HTTPResponseHeaderSetup & http_response_headers_override_,
    const std::string & url_prefix_,
    HTTPPathHintsPtr path_hints_)
    : HTTPHandler(server_, connection_config_, "DynamicQueryHandler", http_response_headers_override_, url_prefix_, std::move(path_hints_))
    , param_name(param_name_)
{
}

bool DynamicQueryHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (key == param_name)
        return true;    /// do nothing

    if (startsWith(key, QUERY_PARAMETER_NAME_PREFIX))
    {
        /// Save name and values of substitution in dictionary.
        const String parameter_name = key.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));

        if (!context->getQueryParameters().contains(parameter_name))
            context->setQueryParameter(parameter_name, value);
        return true;
    }

    return false;
}

std::string DynamicQueryHandler::getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context)
{
    if (likely(!startsWith(request.getContentType(), "multipart/form-data")))
    {
        /// Part of the query can be passed in the 'query' parameter and the rest in the request body
        /// (http method need not necessarily be POST). In this case the entire query consists of the
        /// contents of the 'query' parameter, a line break and the request body.
        std::string query_param = params.get(param_name, "");
        return query_param.empty() ? query_param : query_param + "\n";
    }

    /// Support for "external data for query processing".
    /// Used in case of POST request with form-data, but it isn't expected to be deleted after that scope.
    ExternalTablesHandler handler(context, params);
    auto input_stream = request.getStream();
    params.load(request, *input_stream, handler);

    std::string full_query;
    /// Params are of both form params POST and uri (GET params)
    for (const auto & it : params)
    {
        if (it.first == param_name)
        {
            full_query += it.second;
        }
        else
        {
            customizeQueryParam(context, it.first, it.second);
        }
    }

    return full_query;
}

PredefinedQueryHandler::PredefinedQueryHandler(
    IServer & server_,
    const HTTPHandlerConnectionConfig & connection_config,
    const NameSet & receive_params_,
    const std::string & predefined_query_,
    const CompiledRegexPtr & url_regexp_,
    const std::unordered_map<String, CompiledRegexPtr> & header_name_with_regexp_,
    const HTTPResponseHeaderSetup & http_response_headers_override_)
    : HTTPHandler(server_, connection_config, "PredefinedQueryHandler", http_response_headers_override_)
    , receive_params(receive_params_)
    , predefined_query(predefined_query_)
    , url_regexp(url_regexp_)
    , header_name_with_capture_regexp(header_name_with_regexp_)
{
}

bool PredefinedQueryHandler::customizeQueryParam(ContextMutablePtr context, const std::string & key, const std::string & value)
{
    if (receive_params.contains(key))
    {
        context->setQueryParameter(key, value);
        return true;
    }

    if (startsWith(key, QUERY_PARAMETER_NAME_PREFIX))
    {
        /// Save name and values of substitution in dictionary.
        const String parameter_name = key.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));

        if (receive_params.contains(parameter_name))
            context->setQueryParameter(parameter_name, value);
        return true;
    }

    return false;
}

void PredefinedQueryHandler::customizeContext(HTTPServerRequest & request, ContextMutablePtr context, ReadBuffer & body)
{
    /// If in the configuration file, the handler's header is regex and contains named capture group
    /// We will extract regex named capture groups as query parameters

    const auto & set_query_params = [&](const char * begin, const char * end, const CompiledRegexPtr & compiled_regex)
    {
        int num_captures = compiled_regex->NumberOfCapturingGroups() + 1;

        std::vector<std::string_view> matches(num_captures);
        std::string_view input(begin, end - begin);
        if (compiled_regex->Match(input, 0, end - begin, re2::RE2::Anchor::ANCHOR_BOTH, matches.data(), num_captures))
        {
            for (const auto & [capturing_name, capturing_index] : compiled_regex->NamedCapturingGroups())
            {
                const auto & capturing_value = matches[capturing_index];

                if (capturing_value.data())
                    context->setQueryParameter(capturing_name, String(capturing_value.data(), capturing_value.size()));
            }
        }
    };

    if (url_regexp)
    {
        const auto & uri = request.getURI();
        set_query_params(uri.data(), find_first_symbols<'?'>(uri.data(), uri.data() + uri.size()), url_regexp);
    }

    for (const auto & [header_name, regex] : header_name_with_capture_regexp)
    {
        /// Use a defaulted lookup like `headersFilter` does: a missing header is treated as an empty string.
        /// A header regex can match the empty string, so the rule may match a request without that header,
        /// and reading it here without a default would raise an exception after the rule has already matched.
        const auto header_value = request.get(header_name, "");
        set_query_params(header_value.data(), header_value.data() + header_value.size(), regex);
    }

    if (unlikely(receive_params.contains("_request_body") && !context->getQueryParameters().contains("_request_body")))
    {
        WriteBufferFromOwnString value;
        const auto & settings = context->getSettingsRef();

        copyDataMaxBytes(body, value, settings[Setting::http_max_request_param_data_size]);
        context->setQueryParameter("_request_body", value.str());
    }
}

std::string PredefinedQueryHandler::getQuery(HTTPServerRequest & request, HTMLForm & params, ContextMutablePtr context)
{
    if (unlikely(startsWith(request.getContentType(), "multipart/form-data")))
    {
        /// Support for "external data for query processing".
        ExternalTablesHandler handler(context, params);
        auto input_stream = request.getStream();
        params.load(request, *input_stream, handler);
    }

    return predefined_query;
}

HTTPRequestHandlerFactoryPtr createDynamicHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    std::unordered_map<String, String> & common_headers)
{
    auto query_param_name = config.getString(config_prefix + ".handler.query_param_name", "query");

    /// Optional URL prefix under which this handler is mounted. When set, the prefix is required
    /// to match incoming requests and is stripped before the remaining URL path is parsed as
    /// `database/table.format[.compression]` (or filters / hive partitions). Example config:
    ///   <url_prefix>/api</url_prefix>
    /// A request `GET /api/db/hits.csv` is then processed as if the path were `/db/hits.csv`.
    auto url_prefix = config.getString(config_prefix + ".url_prefix", "");
    /// Normalize the prefix once (strip a trailing slash) before it is used for BOTH the routing filter
    /// below and path stripping in the handler. `addFiltersFromConfig` already installs a `url_prefix`
    /// filter that tolerates a trailing slash (it matches `/api/v1?query=...` for `<url_prefix>/api/v1/`),
    /// so without normalizing, the extra raw-prefix `hasUrlPrefixWithSegmentBoundary` filter below would
    /// reject that same request — a regression for configured prefixes written with a trailing slash.
    while (url_prefix.size() > 1 && url_prefix.ends_with('/'))
        url_prefix.pop_back();

    HTTPHandlerConnectionConfig connection_config(config, config_prefix);
    HTTPResponseHeaderSetup http_response_headers_override = parseHTTPResponseHeaders(config, config_prefix);
    if (!common_headers.empty())
    {
        if (!http_response_headers_override.has_value())
            http_response_headers_override.emplace();
        http_response_headers_override.value().insert(common_headers.begin(), common_headers.end());
    }

    auto creator = [&server, query_param_name, http_response_headers_override, connection_config, url_prefix]() -> std::unique_ptr<DynamicQueryHandler>
    { return std::make_unique<DynamicQueryHandler>(server, connection_config, query_param_name, http_response_headers_override, url_prefix); };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<DynamicQueryHandler>>(std::move(creator));
    factory->addFiltersFromConfig(config, config_prefix);
    if (!url_prefix.empty())
        factory->addFilter([url_prefix](const auto & request)
        { return hasUrlPrefixWithSegmentBoundary(request.getURI(), url_prefix); });
    return factory;
}

HTTPRequestHandlerFactoryPtr createPredefinedHandlerFactory(IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    std::unordered_map<String, String> & common_headers)
{
    if (!config.has(config_prefix + ".handler.query"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no path '{}.handler.query' in configuration file.", config_prefix);

    std::string predefined_query = config.getString(config_prefix + ".handler.query");
    /// Remove leading and trailing whitespace that may come from XML formatting in the config file.
    /// This prevents whitespace from being interpreted as data for binary formats like MsgPack.
    boost::algorithm::trim(predefined_query);
    NameSet analyze_receive_params = analyzeReceiveQueryParams(predefined_query);

    HTTPHandlerConnectionConfig connection_config(config, config_prefix);

    /// Regular expressions from the rule's url/headers whose named capturing groups are referenced by the query;
    /// their captured values are passed to the query as parameters by PredefinedQueryHandler::customizeContext.
    auto regexps = HTTPHandlerRegexpsWithNamedGroups::fromConfig(config, config_prefix, analyze_receive_params);

    HTTPResponseHeaderSetup http_response_headers_override = parseHTTPResponseHeaders(config, config_prefix);
    if (!common_headers.empty())
    {
        if (!http_response_headers_override.has_value())
            http_response_headers_override.emplace();
        http_response_headers_override.value().insert(common_headers.begin(), common_headers.end());
    }

    auto creator = [
        &server,
        analyze_receive_params,
        predefined_query,
        url_regexp = regexps.url_regexp,
        headers_name_with_regexp = std::move(regexps.headers_name_with_regexp),
        http_response_headers_override,
        connection_config]
        -> std::unique_ptr<PredefinedQueryHandler>
    {
        return std::make_unique<PredefinedQueryHandler>(
            server,
            connection_config,
            analyze_receive_params,
            predefined_query,
            url_regexp,
            headers_name_with_regexp,
            http_response_headers_override);
    };
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PredefinedQueryHandler>>(std::move(creator));
    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

void HTTPHandler::Output::pushDelayedResults() const
{
    auto * cascade_buffer = typeid_cast<CascadeWriteBuffer *>(out_maybe_delayed_and_compressed.get());
    if (!cascade_buffer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected CascadeWriteBuffer");

    cascade_buffer->finalize();
    auto write_buffers = cascade_buffer->getResultBuffers();

    if (write_buffers.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "At least one buffer is expected to overwrite result into HTTP response");

    ConcatReadBuffer::Buffers read_buffers;
    for (auto & write_buf : write_buffers)
    {
        if (auto * write_buf_concrete = dynamic_cast<TemporaryDataBuffer *>(write_buf.get()))
        {
            if (auto reread_buf = write_buf_concrete->read())
            {
                read_buffers.emplace_back(std::move(reread_buf));
            }
        }

        if (auto * write_buf_concrete = dynamic_cast<IReadableWriteBuffer *>(write_buf.get()))
        {
            if (auto reread_buf = write_buf_concrete->tryGetReadBuffer())
            {
                read_buffers.emplace_back(std::move(reread_buf));
            }
        }
    }

    if (!read_buffers.empty())
    {
        ConcatReadBuffer concat_read_buffer(std::move(read_buffers));
        copyData(concat_read_buffer, *out_maybe_compressed);
    }
}

void HTTPHandler::Output::finalize()
{
    if (finalized)
        return;
    finalized = true;

    if (hasDelayed())
        pushDelayedResults();

    if (out_delayed_and_compressed_holder)
        out_delayed_and_compressed_holder->finalize();
    if (out_compressed_holder)
        out_compressed_holder->finalize();
    if (generic_compression_holder)
        generic_compression_holder->finalize();
    if (wrap_compressed_holder)
        wrap_compressed_holder->finalize();
    if (out_holder)
        out_holder->finalize();
}

void HTTPHandler::Output::cancel()
{
    if (canceled)
        return;
    canceled = true;

    if (out_delayed_and_compressed_holder)
        out_delayed_and_compressed_holder->cancel();
    if (out_compressed_holder)
        out_compressed_holder->cancel();
    if (generic_compression_holder)
        generic_compression_holder->cancel();
    if (wrap_compressed_holder)
        wrap_compressed_holder->cancel();
    if (out_holder)
        out_holder->cancel();
}
}
