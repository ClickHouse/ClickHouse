#pragma once
#include <Common/SettingsChanges.h>
#include <base/types.h>

#include <ctime>
#include <istream>
#include <optional>

namespace Poco::Net
{
class HTTPResponse;
}

namespace DB
{
/// Class for parsing optional metadata returned by an HTTP authentication server.
class SettingsAuthResponseParser
{
    static constexpr auto settings_key = "settings";
    static constexpr auto roles_key = "roles";
    static constexpr auto valid_until_key = "valid_until";

public:
    struct Result
    {
        bool is_ok = false;
        SettingsChanges settings;
        Strings roles;
        std::optional<time_t> valid_until;
    };

    Result parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const;
};

}
