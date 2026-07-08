#include <Server/HTTP/setReadOnlyIfHTTPMethodIdempotent.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPServerRequest.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 readonly;
}

void setReadOnlyIfHTTPMethodIdempotent(ContextMutablePtr context, const String & http_method, bool allow_mutating_idempotent_methods)
{
    /// HTTP POST is always allowed to run modifying queries. Every other method - most importantly the safe
    /// methods GET and HEAD - implies a readonly query by default.
    /// PUT and DELETE are treated as mutating only when `allow_mutating_idempotent_methods` is set, which happens
    /// exclusively for SQL-defined handlers (see `CREATE HANDLER`) that explicitly accept them. Config-defined
    /// `dynamic_query_handler`/`predefined_query_handler` rules and the built-in `/` endpoint keep the original
    /// POST-only behavior, so a PUT/DELETE request they match still forces readonly.
    const bool is_mutating_method =
        http_method == HTTPServerRequest::HTTP_POST
        || (allow_mutating_idempotent_methods
            && (http_method == HTTPServerRequest::HTTP_PUT || http_method == HTTPServerRequest::HTTP_DELETE));

    if (!is_mutating_method)
    {
        /// 'readonly' setting values mean:
        /// readonly = 0 - any query is allowed, client can change any setting.
        /// readonly = 1 - only readonly queries are allowed, client can't change settings.
        /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.
        if (context->getSettingsRef()[Setting::readonly] == 0)
            context->setSetting("readonly", 2);
    }
}

}
