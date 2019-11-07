#pragma once

#include <Poco/Net/HTTPServerRequest.h>
#include <Core/ExternalTable.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/CustomExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_HTTP_PARAM;
}

class ExtractorContextChange
{
public:
    ExtractorContextChange(Context & context_, const CustomExecutorPtr & executor_) : context(context_), executor(executor_) {}

    static const NameSet & getReservedParamNames()
    {
        static const NameSet reserved_param_names{
            "compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace",
            "buffer_size", "wait_end_of_query", "session_id", "session_timeout", "session_check"
        };

        return reserved_param_names;
    }

    static std::function<bool(const String &)> reservedParamSuffixesFilter(bool reserved)
    {
        if (!reserved)
            return [&](const String &) { return false; };

        /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
        /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
        return [&](const String & param_name)
        {
            if (endsWith(param_name, "_format"))
                return true;
            else if (endsWith(param_name, "_types"))
                return true;
            else if (endsWith(param_name, "_structure"))
                return true;

            return false;
        };
    }

    void extract(Poco::Net::HTTPServerRequest & request, HTMLForm & params)
    {
        bool is_multipart_data = startsWith(request.getContentType().data(), "multipart/form-data");

        /// Settings can be overridden in the query.
        /// Some parameters (database, default_format, everything used in the code above) do not
        /// belong to the Settings class.
        becomeReadonlyIfNeed(request);
        changeSettingsFromParams(params, reservedParamSuffixesFilter(is_multipart_data));

        if (is_multipart_data || executor->canBeParseRequestBody())
        {
            ExternalTablesHandler handler(context, params);
            params.load(request, request.stream(), handler);

            /// We use the `Post Request Body Settings` to override the `Qeruy String Param settings`
            if (executor->canBeParseRequestBody())
                changeSettingsFromParams(params, reservedParamSuffixesFilter(is_multipart_data));
        }
    }

private:
    Context & context;
    const CustomExecutorPtr & executor;

    /// 'readonly' setting values mean:
    /// readonly = 0 - any query is allowed, client can change any setting.
    /// readonly = 1 - only readonly queries are allowed, client can't change settings.
    /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.

    /// In theory if initially readonly = 0, the client can change any setting and then set readonly
    /// to some other value.
    /// Only readonly queries are allowed for HTTP GET requests.
    void becomeReadonlyIfNeed(HTTPRequest & request)
    {
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        {
            Settings & settings = context.getSettingsRef();

            if (settings.readonly == 0)
                settings.readonly = 2;
        }
    }


    void changeSettingsFromParams(HTMLForm & params, const std::function<bool(const String &)> & reserved_param_suffixes)
    {
        SettingsChanges settings_changes;
        const auto & reserved_param_names = getReservedParamNames();

        for (const auto & [name, value] : params)
        {
            if (name == "database")
                context.setCurrentDatabase(value);
            else if (name == "default_format")
                context.setDefaultFormat(value);
            else if (!reserved_param_names.count(name) && !reserved_param_suffixes(name))
            {
                if (Settings::findIndex(name) != Settings::npos)
                    settings_changes.push_back({name, value});
                else if (!executor->isQueryParam(name))
                    throw Exception("Unknown HTTP param name: '" + name + "'", ErrorCodes::UNKNOWN_HTTP_PARAM);
            }
        }

        /// For external data we also want settings
        context.checkSettingsConstraints(settings_changes);
        context.applySettingsChanges(settings_changes);
    }
};

}
