#pragma once

#include <ctime>
#include <istream>

#include <Common/SettingsChanges.h>
#include <Core/Field.h>
#include <Core/Types.h>

namespace Poco::Net { class HTTPResponse; }
namespace Poco::Dynamic { class Var; }

namespace DB
{

/// Strict response parser for the `http` external user directory.
///
/// Unlike `SettingsAuthResponseParser` (used by pre-created users with `IDENTIFIED WITH HTTP`,
/// where malformed response metadata is silently skipped), any malformed or invalid metadata
/// here fails the whole authentication attempt: silently dropping security metadata could hide
/// a compromised or misconfigured authentication service.
///
/// This is a protocol parser only: `settings` entries are returned by name with their JSON scalar
/// value as a `Field` (string, integer, float or boolean; anything else fails). Whether a name is an
/// allowed setting (built-in, or matching `custom_settings_prefixes`) and how a built-in setting
/// interprets the value is decided by `HTTPAccessStorage`, which owns the `AccessControl` policy.
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
        /// Raw entries: names unvalidated, values as JSON scalars.
        SettingsChanges settings;
        Strings role_names;
        /// Absolute Unix timestamp in seconds; 0 means no expiry.
        time_t valid_until = 0;
    };

    Result parse(const Poco::Net::HTTPResponse & response, std::istream * body_stream) const;

private:
    /// Reads the whole body (so the connection stays reusable), failing once it exceeds a fixed limit.
    static String readBoundedBody(std::istream * body_stream);
    static Field jsonScalarToField(const Poco::Dynamic::Var & value, const String & name);
};

}
