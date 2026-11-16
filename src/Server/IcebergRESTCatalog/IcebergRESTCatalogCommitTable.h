#pragma once

#include <base/types.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <expected>

namespace DB::IcebergRESTCatalog
{

/// Checks CommitTableRequest `requirements`. Returns the reason of the first failed requirement, if any.
std::expected<void, String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements);

/// Applies CommitTableRequest `updates` to `metadata` in place.
/// On exception, `metadata` may be partially updated.
void applyTableUpdatesToJSON(Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location);

/// `<location>/metadata/v<N+1>-<random uuid><compression suffix>.metadata.json`, where N is parsed from the current file name.
String nextMetadataLocation(
    const Poco::JSON::Object & metadata, const String & current_metadata_location, const String & compression_suffix);

}
