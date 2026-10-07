#pragma once

#include <base/types.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <expected>

namespace DB::IcebergRESTCatalog
{

/// True if `requirements` contain `assert-create`. Such a commit creates the table.
bool hasAssertCreate(const Poco::JSON::Array & requirements);

/// Checks CommitTableRequest `requirements` against an existing table.
/// Returns the reason of the first failed requirement, if any.
std::expected<void, String> checkTableRequirements(const Poco::JSON::Object & metadata, const Poco::JSON::Array & requirements);

/// Checks CommitTableRequest `requirements` for a table that does not exist. Only `assert-create` passes.
/// Returns the reason of the first failed requirement, if any.
std::expected<void, String> checkCreateRequirements(const Poco::JSON::Array & requirements);

/// Applies CommitTableRequest `updates` to `metadata` in place.
/// On exception, `metadata` may be partially updated.
void applyTableUpdatesToJSON(Poco::JSON::Object & metadata, const Poco::JSON::Array & updates, const String & current_metadata_location);

/// Builds the metadata of a staged table from the `updates` of its create commit.
Poco::JSON::Object::Ptr buildCreatedTableMetadata(const Poco::JSON::Array & updates);

/// `<location>/metadata/v<N+1>-<random uuid><compression suffix>.metadata.json`, where N is parsed from the current file name.
String nextMetadataLocation(
    const Poco::JSON::Object & metadata, const String & current_metadata_location, const String & compression_suffix);

}
