#pragma once

#include <Poco/Net/HTTPServerRequest.h>
#include <Core/ExternalTable.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/Context.h>
#include <Interpreters/CustomHTTP/CustomExecutor.h>

namespace DB
{

class ExtractorContextChange
{
public:
    ExtractorContextChange(Context & context_, const CustomExecutorPtr & executor_) : context(context_), executor(executor_) {}

    void extract(Poco::Net::HTTPServerRequest & request, HTMLForm & params)
    {
        Names reserved_param_suffixes;
        static const NameSet reserved_param_names{
            "compress", "decompress", "user", "password", "quota_key", "query_id", "stacktrace", "buffer_size", "wait_end_of_query",
            "session_id", "session_timeout", "session_check"};

        auto param_could_be_skipped = [&] (const String & name)
        {
            if (reserved_param_names.count(name))
                return true;

            for (const String & suffix : reserved_param_suffixes)
            {
                if (endsWith(name, suffix))
                    return true;
            }

            return false;
        };

        /// Settings can be overridden in the query.
        /// Some parameters (database, default_format, everything used in the code above) do not
        /// belong to the Settings class.

        /// 'readonly' setting values mean:
        /// readonly = 0 - any query is allowed, client can change any setting.
        /// readonly = 1 - only readonly queries are allowed, client can't change settings.
        /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.

        /// In theory if initially readonly = 0, the client can change any setting and then set readonly
        /// to some other value.
        /// Only readonly queries are allowed for HTTP GET requests.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET)
        {
            Settings & settings = context.getSettingsRef();

            if (settings.readonly == 0)
                settings.readonly = 2;
        }

        bool has_multipart = startsWith(request.getContentType().data(), "multipart/form-data");

        if (has_multipart || executor->canBeParseRequestBody(request, params))
        {
            ExternalTablesHandler handler(context, params);
            params.load(request, request.stream(), handler);

            if (has_multipart)
            {
                /// Skip unneeded parameters to avoid confusing them later with context settings or query parameters.
                reserved_param_suffixes.reserve(3);
                /// It is a bug and ambiguity with `date_time_input_format` and `low_cardinality_allow_in_native_format` formats/settings.
                reserved_param_suffixes.emplace_back("_format");
                reserved_param_suffixes.emplace_back("_types");
                reserved_param_suffixes.emplace_back("_structure");
            }
        }

        SettingsChanges settings_changes;
        for (const auto & [key, value] : params)
        {
            if (key == "database")
                context.setCurrentDatabase(value);
            else if (key == "default_format")
                context.setDefaultFormat(value);
            else if (!param_could_be_skipped(key) && !executor->isQueryParam(key))
                settings_changes.push_back({key, value}); /// All other query parameters are treated as settings.
        }

        /// For external data we also want settings
        context.checkSettingsConstraints(settings_changes);
        context.applySettingsChanges(settings_changes);
    }

private:
    Context & context;
    const CustomExecutorPtr & executor;

};

}
