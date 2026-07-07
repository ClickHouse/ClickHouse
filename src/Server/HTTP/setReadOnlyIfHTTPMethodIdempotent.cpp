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

void setReadOnlyIfHTTPMethodIdempotent(ContextMutablePtr context, const String & http_method)
{
    /// The mutating HTTP methods (POST, PUT, DELETE) are allowed to run modifying queries.
    /// Every other method - most importantly the safe methods GET and HEAD - implies a readonly query.
    /// PUT and DELETE are relevant for SQL-defined handlers (see `CREATE HANDLER`) that explicitly accept them;
    /// the built-in `/` endpoint only routes GET/HEAD/POST/OPTIONS, so this does not change its behavior.
    const bool is_mutating_method =
        http_method == HTTPServerRequest::HTTP_POST
        || http_method == HTTPServerRequest::HTTP_PUT
        || http_method == HTTPServerRequest::HTTP_DELETE;

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
