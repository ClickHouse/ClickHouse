#include <Access/SettingsAuthResponseParser.h>

#include <Access/resolveSetting.h>
#include <Common/Exception.h>
#include <IO/HTTPCommon.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
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

    Poco::JSON::Object::Ptr obj;

    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(*body_stream);
        obj = json.extract<Poco::JSON::Object::Ptr>();
    }
    catch (...)
    {
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse authentication response body. Skip optional response metadata.");
        return result;
    }

    try
    {
        if (auto settings_obj = obj->getObject(settings_key))
        {
            /// Append directly to preserve the existing behavior where settings parsed before a later
            /// conversion failure remain in the result.
            for (const auto & [key, value] : *settings_obj)
                result.settings.emplace_back(key, settingStringToValueUtil(key, value));
        }
    }
    catch (...)
    {
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse settings from authentication response. Skip them.");
    }

    try
    {
        if (obj->has(roles_key))
        {
            if (!obj->isArray(roles_key))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Expected `roles` in authentication response to be an array");

            auto roles_array = obj->getArray(roles_key);
            Strings parsed_roles;
            parsed_roles.reserve(roles_array->size());

            for (const auto & value : *roles_array)
            {
                if (!value.isString())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Expected every `roles` element in authentication response to be a string");

                parsed_roles.emplace_back(value.extract<String>());
            }

            result.roles = std::move(parsed_roles);
        }
    }
    catch (...)
    {
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse roles from authentication response. Skip them.");
    }

    try
    {
        if (obj->has(valid_until_key))
        {
            const auto value = obj->get(valid_until_key);
            if (!value.isInteger() || value.isBoolean())
                throw Exception(ErrorCodes::INCORRECT_DATA, "Expected `valid_until` in authentication response to be an integer");

            const Int64 timestamp = value.convert<Int64>();
            if (!std::in_range<time_t>(timestamp))
                throw Exception(ErrorCodes::INCORRECT_DATA, "The `valid_until` value in authentication response is outside the `time_t` range");

            result.valid_until = static_cast<time_t>(timestamp);
        }
    }
    catch (...)
    {
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse valid_until from authentication response. Skip it.");
    }

    return result;
}

}
