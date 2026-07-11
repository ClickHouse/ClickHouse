#pragma once

#include "config.h"

#if USE_SILK

#include <base/types.h>

namespace DB::Proxy
{

class Router;

/// Render a JSON document describing all pools, their backends, and the health and statistics of each.
/// Served on the HTTP status endpoint.
String buildStatusJSON(const Router & router);

}

#endif
