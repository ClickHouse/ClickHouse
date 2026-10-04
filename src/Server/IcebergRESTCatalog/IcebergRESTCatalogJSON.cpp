#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>

#include <Common/Exception.h>

#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

String toJSONString(const Poco::JSON::Object & json, unsigned indent)
{
    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(json, oss, indent);
    return oss.str();
}

Poco::JSON::Object::Ptr parseJSONObject(const String & data, const String & what)
{
    try
    {
        return Poco::JSON::Parser().parse(data).extract<Poco::JSON::Object::Ptr>();
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::INCORRECT_DATA, "{} is not a JSON object: {}", what, e.displayText());
    }
}

}
