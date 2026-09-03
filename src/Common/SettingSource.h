#pragma once

#include <string_view>

namespace DB
{
    enum SettingSource
    {
        /// Query or session change:
        /// SET <setting> = <value>
        /// SELECT ... SETTINGS [<setting> = <value]
        QUERY,

        /// Profile creation or altering:
        /// CREATE SETTINGS PROFILE ... SETTINGS [<setting> = <value]
        /// ALTER SETTINGS PROFILE ... SETTINGS [<setting> = <value]
        PROFILE,

        /// Role creation or altering:
        /// CREATE ROLE ... SETTINGS [<setting> = <value>]
        /// ALTER ROLE ... SETTINGS [<setting> = <value]
        ROLE,

        /// User creation or altering:
        /// CREATE USER ... SETTINGS [<setting> = <value>]
        /// ALTER USER ... SETTINGS [<setting> = <value]
        USER,

        COUNT,
    };

    constexpr std::string_view toString(SettingSource source)
    {
        switch (source)
        {
            case SettingSource::QUERY: return "query";
            case SettingSource::PROFILE: return "profile";
            case SettingSource::USER: return "user";
            case SettingSource::ROLE: return "role";
            default: return "unknown";
        }
    }
}
