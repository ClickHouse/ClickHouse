#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{

/// Sets readonly = 2 for safe HTTP methods (everything except the mutating ones) if readonly is not set already.
/// By default only HTTP POST is treated as mutating. When `allow_mutating_idempotent_methods` is true, PUT and
/// DELETE are treated as mutating too; this is enabled only for SQL-defined handlers (see `CREATE HANDLER`) that
/// explicitly accept those methods, and must stay disabled for config-defined and built-in handlers.
void setReadOnlyIfHTTPMethodIdempotent(
    ContextMutablePtr context, const String & http_method, bool allow_mutating_idempotent_methods = false);

}
