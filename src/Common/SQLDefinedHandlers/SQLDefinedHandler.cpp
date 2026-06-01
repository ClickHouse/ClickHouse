#include <Common/SQLDefinedHandlers/SQLDefinedHandler.h>

#include <Common/StringUtils.h>

#include <algorithm>


namespace DB
{

bool SQLDefinedHandler::matchesURL(const String & path) const
{
    switch (url_match_type)
    {
        case URLMatchType::Exact:
            return path == url;
        case URLMatchType::Prefix:
            return startsWith(path, url);
        case URLMatchType::Regexp:
            return url_regex && re2::RE2::FullMatch(re2::StringPiece(path.data(), path.size()), *url_regex);
    }
}

bool SQLDefinedHandler::matchesMethod(const String & method) const
{
    return std::find(methods.begin(), methods.end(), method) != methods.end();
}

bool SQLDefinedHandler::matchesProtocol(const String & protocol_name) const
{
    if (!protocol)
        return true;
    return *protocol == protocol_name;
}

}
