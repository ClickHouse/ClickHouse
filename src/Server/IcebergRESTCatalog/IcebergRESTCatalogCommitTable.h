#pragma once

#include <base/types.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <optional>

namespace DB
{

/// Checks CommitTableRequest `requirements`. Returns the reason of the first failed requirement, if any.
std::optional<String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements);

/// Applies CommitTableRequest `updates` to `metadata` in place.
/// Throws `BAD_ARGUMENTS` for an invalid update and `NOT_IMPLEMENTED` for an unsupported one.
/// On exception, `metadata` may be partially updated.
void applyTableUpdates(Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location);

/// `<location>/metadata/v<N+1>-<random uuid><compression suffix>.metadata.json`, where N is parsed from the current file name.
String nextMetadataLocation(
    const Poco::JSON::Object & metadata, const String & current_metadata_location, const String & compression_suffix);

}
