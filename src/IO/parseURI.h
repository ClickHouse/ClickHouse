#pragma once

#include <base/types.h>
#include <Poco/URI.h>

namespace DB
{

/// Like the `Poco::URI` constructor, but a malformed string is reported as `BAD_ARGUMENTS`.
Poco::URI parseURI(const String & uri, bool enable_url_encoding = true);

/// Like `Poco::URI::getQueryParameters`, but a malformed percent escape is reported as `BAD_ARGUMENTS`.
Poco::URI::QueryParameters getURIQueryParameters(const Poco::URI & uri);

}
