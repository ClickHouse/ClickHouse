#include <Server/IcebergRESTCatalog/IcebergRESTCatalogJSON.h>

#include <Common/Exception.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

namespace IcebergRESTCatalogJSON
{

Poco::JSON::Object::Ptr getOptionalObject(const Poco::JSON::Object & json, const String & key)
{
    if (!json.has(key) || json.isNull(key))
        return nullptr;
    auto object = json.getObject(key);
    if (!object)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' must be an object", key);
    return object;
}

Int64 getInteger(const Poco::JSON::Object & object, const String & key)
{
    if (!object.has(key) || !object.get(key).isInteger())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or non-integer '{}'", key);
    return object.getValue<Int64>(key);
}

Poco::JSON::Array::Ptr getArray(const Poco::JSON::Object & object, const String & key)
{
    auto array = object.getArray(key);
    if (!array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or non-array '{}'", key);
    return array;
}

Poco::Dynamic::Var getValue(const Poco::JSON::Object & object, const String & key)
{
    if (!object.has(key))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing '{}'", key);
    return object.get(key);
}

void validateBoolean(const Poco::JSON::Object & object, const String & key)
{
    if (!object.has(key) || !object.get(key).isBoolean())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or non-boolean '{}'", key);
}

void validateString(const Poco::JSON::Object & object, const String & key)
{
    if (!object.has(key) || !object.get(key).isString() || object.getValue<String>(key).empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Missing or empty string '{}'", key);
}

Poco::JSON::Array::Ptr makeArray(const Poco::JSON::Object::Ptr & element)
{
    Poco::JSON::Array::Ptr array = new Poco::JSON::Array;
    array->add(element);
    return array;
}

}

}
