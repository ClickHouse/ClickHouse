#pragma once

#include <base/types.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <map>
#include <optional>

namespace DB
{

/// Builds the `metadata.json` of a new table from a CreateTableRequest. Throws `BAD_ARGUMENTS` for an invalid request.
/// `partition_spec` and `write_order` may be nullptr.
Poco::JSON::Object::Ptr buildInitialTableMetadata(
    const String & uuid,
    const String & location,
    Poco::JSON::Object::Ptr schema,
    Poco::JSON::Object::Ptr partition_spec,
    Poco::JSON::Object::Ptr write_order,
    std::map<String, String> properties);

/// Checks CommitTableRequest `requirements`. Returns reason of first failed requirement, if any.
std::optional<String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements);

/// Applies CommitTableRequest `updates` array to copy of `metadata`. Returns copy.
Poco::JSON::Object::Ptr applyTableUpdates(
    const Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location);

/// `<location>/metadata/v<N+1>-<random uuid>.metadata.json`, where N is parsed from the current file name.
String nextMetadataLocation(const Poco::JSON::Object & metadata, const String & current_metadata_location);

}
