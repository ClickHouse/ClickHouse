#include <Access/SettingsAuthResponseParser.h>

#include <Access/resolveSetting.h>
#include <IO/HTTPCommon.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

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
    Poco::JSON::Object::Ptr parsed_body;

    try
    {
        Poco::Dynamic::Var json = parser.parse(*body_stream);
        const Poco::JSON::Object::Ptr & obj = json.extract<Poco::JSON::Object::Ptr>();
        Poco::JSON::Object::Ptr settings_obj = obj->getObject(settings_key);

        if (settings_obj)
            for (const auto & [key, value] : *settings_obj)
                result.settings.emplace_back(key, settingStringToValueUtil(key, value));
    }
    catch (...)
    {
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse settings from authentication response. Skip it.");
    }
    return result;
}

}
