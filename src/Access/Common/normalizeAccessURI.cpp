#include <Access/Common/normalizeAccessURI.h>

#include <Poco/Exception.h>
#include <Poco/URI.h>

namespace DB
{

String normalizeAccessURI(const String & uri)
{
    try
    {
        Poco::URI parsed(uri);
        parsed.normalize();
        return parsed.toString();
    }
    catch (const Poco::Exception &)
    {
        return "";
    }
}

}
