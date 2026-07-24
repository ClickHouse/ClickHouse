#include <Access/SettingsAuthResponseParser.h>

#include <Access/resolveSetting.h>
#include <Common/Exception.h>
#include <Formats/ParseError.h>
#include <IO/HTTPCommon.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int CANNOT_RESTORE_FROM_FIELD_DUMP;
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int UNKNOWN_SETTING;
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
    Poco::JSON::Object::Ptr parsed_body;

    try
    {
        Exception::SuppressErrorCodesScope suppress_error_codes;
        Poco::Dynamic::Var json = parser.parse(*body_stream);
        const Poco::JSON::Object::Ptr & obj = json.extract<Poco::JSON::Object::Ptr>();
        Poco::JSON::Object::Ptr settings_obj = obj->getObject(settings_key);

        if (settings_obj)
            for (const auto & [key, value] : *settings_obj)
                result.settings.emplace_back(key, settingStringToValueUtil(key, value));
    }
    catch (Exception & e)
    {
        if (!isParseError(e.code())
            && e.code() != ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
            && e.code() != ErrorCodes::BAD_ARGUMENTS
            && e.code() != ErrorCodes::CANNOT_CONVERT_TYPE
            && e.code() != ErrorCodes::CANNOT_RESTORE_FROM_FIELD_DUMP
            && e.code() != ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH
            && e.code() != ErrorCodes::UNKNOWN_SETTING)
            e.recordToSystemErrors(/* force */ true);
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse settings from authentication response. Skip it.");
    }
    catch (...)
    {
        LOG_INFO(getLogger("HTTPAuthentication"), "Failed to parse settings from authentication response. Skip it.");
    }
    return result;
}

}
