#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestError.h>

#if USE_AVRO

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <sstream>

namespace DataLake::IcebergRestModels
{

namespace
{

std::optional<ErrorResponse> parseErrorModel(const Poco::JSON::Object::Ptr & object)
{
    if (!object || !object->has("message"))
        return std::nullopt;

    ErrorResponse error;
    error.message = object->get("message").extract<std::string>();
    if (object->has("type"))
        error.type = object->get("type").extract<std::string>();
    if (object->has("code"))
        error.code = object->getValue<int>("code");
    return error;
}

Poco::JSON::Object::Ptr serializeErrorModel(const ErrorResponse & error)
{
    Poco::JSON::Object::Ptr object = new Poco::JSON::Object;
    object->set("message", error.message);
    if (!error.type.empty())
        object->set("type", error.type);
    if (error.code)
        object->set("code", error.code);
    return object;
}

}

std::optional<ErrorResponse> tryParseErrorResponse(const std::string & json)
{
    if (json.empty())
        return std::nullopt;

    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var parsed = parser.parse(json);
        const auto & object = parsed.extract<Poco::JSON::Object::Ptr>();

        if (!object->has("error"))
            return std::nullopt;

        return parseErrorModel(object->get("error").extract<Poco::JSON::Object::Ptr>());
    }
    catch (...)
    {
        return std::nullopt;
    }
}

std::string serializeErrorResponse(const ErrorResponse & error)
{
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    root->set("error", serializeErrorModel(error));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(root, oss);
    return oss.str();
}

}

#endif
