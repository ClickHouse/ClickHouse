#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Sets readonly = 2 if the current HTTP method is not HTTP POST and if readonly is not set already.
void setReadOnlyIfHTTPMethodIdempotent(ContextMutablePtr context, const String & http_method);

}
