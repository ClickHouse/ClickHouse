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
    /// Anything else beside HTTP POST should be readonly queries.
    if (http_method != HTTPServerRequest::HTTP_POST)
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
