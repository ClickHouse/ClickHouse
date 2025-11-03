#include <Access/SettingsAuthResponseParser.h>

#include <Access/resolveSetting.h>
#include <IO/HTTPCommon.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace
{
Field jsonValueToField(const Poco::Dynamic::Var & json_value)
{
    if (json_value.isEmpty())
        return Field{};

    if (json_value.isBoolean())
        return Field{json_value.extract<bool>()};

    if (json_value.isString())
        return Field{json_value.extract<String>()};

    if (json_value.isInteger())
    {
        if (json_value.isSigned())
            return Field{json_value.extract<Int64>()};
        else
            return Field{json_value.extract<UInt64>()};
    }

    if (json_value.isNumeric())
        return Field{json_value.extract<Float64>()};

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported JSON value type in HTTP authentication response settings");
}
}

SettingsAuthResponseParser::Result
SettingsAuthResponseParser::parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const
{
    Result result;

    if (response.getStatus() != Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK)
        return result;
    result.is_ok = true;

    if (!body_stream)
        return result;

    Poco::JSON::Parser parser;

    try
    {
        Poco::Dynamic::Var json = parser.parse(*body_stream);
        const Poco::JSON::Object::Ptr & obj = json.extract<Poco::JSON::Object::Ptr>();
        Poco::JSON::Object::Ptr settings_obj = obj->getObject(settings_key);

        if (!settings_obj)
            return result;

        for (const auto & [key, value] : *settings_obj)
        {
            try
            {
                Field field_value = jsonValueToField(value);
                Field setting_value = settingCastValueUtil(key, field_value);
                result.settings.emplace_back(key, setting_value);
            }
            catch (...)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Failed to parse setting '{}' with an error:\n{}",
                    key,
                    getCurrentExceptionMessage(/* with_stacktrace */ true));
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("HTTPAuthentication"), "Failed to parse settings from authentication response. Skip it.");
        result.settings.clear();
    }
    return result;
}

}
