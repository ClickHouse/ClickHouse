#pragma once
#include <Common/SettingsChanges.h>

#include <istream>

namespace Poco::Net
{
class HTTPResponse;
}

namespace DB
{
/// Class for parsing authentication response containing session settings
class SettingsAuthResponseParser
{
    static constexpr auto settings_key = "settings";

public:
    struct Result
    {
        bool is_ok = false;
        SettingsChanges settings;
    };

    Result parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const;
};

}
