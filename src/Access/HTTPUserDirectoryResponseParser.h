#pragma once

#include <ctime>
#include <istream>

#include <Common/SettingsChanges.h>
#include <Core/Types.h>

namespace Poco::Net { class HTTPResponse; }

namespace DB
{

/// Strict response parser for the `http` external user directory.
///
/// Unlike `SettingsAuthResponseParser` (used by pre-created users with `IDENTIFIED WITH HTTP`,
/// where malformed response metadata is silently skipped), any malformed or invalid metadata
/// here fails the whole authentication attempt: silently dropping security metadata could hide
/// a compromised or misconfigured authentication service.
///
/// Status mapping:
///   200 -> Ok (body parsed strictly),
///   404 -> UserNotFound (the caller may try the next access storage),
///   anything else -> exception (fail-closed, no fallback to other storages).
class HTTPUserDirectoryResponseParser
{
public:
    struct Result
    {
        enum class Status
        {
            Ok,
            UserNotFound,
        };

        Status status = Status::UserNotFound;
        SettingsChanges settings;
        Strings role_names;
        /// Absolute Unix timestamp in seconds; 0 means no expiry.
        time_t valid_until = 0;
    };

    Result parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const;
};

}
