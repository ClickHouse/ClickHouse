#pragma once

#include <Server/HTTP/HTTPRequest.h>


namespace DB
{

/// Checks whether we should defer the HTTP 100 Continue response to after quota checks.
bool shouldDeferHTTP100Continue(const HTTPRequest & request);

}
