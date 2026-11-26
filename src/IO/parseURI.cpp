#include <IO/parseURI.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

Poco::URI parseURI(const String & uri, bool enable_url_encoding)
{
    try
    {
        return Poco::URI(uri, enable_url_encoding);
    }
    catch (const Poco::SyntaxException & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse URI: {}", e.displayText());
    }
}

Poco::URI::QueryParameters getURIQueryParameters(const Poco::URI & uri)
{
    try
    {
        return uri.getQueryParameters();
    }
    catch (const Poco::SyntaxException & e)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse URI: {}", e.displayText());
    }
}

}
