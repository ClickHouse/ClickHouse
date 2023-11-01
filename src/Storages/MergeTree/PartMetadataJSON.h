#pragma once

#include <base/strong_typedef.h>
#include <base/types.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>

namespace DB
{

STRONG_TYPEDEF(UInt32, PartMetadataFormatVersion)

const PartMetadataFormatVersion PART_METADATA_FORMAT_VERSION_OLD {0};
const PartMetadataFormatVersion PART_METADATA_FORMAT_VERSION_INITIAL {1};

// For now the default version is held back, while the feature is still considered experimental
const PartMetadataFormatVersion PART_METADATA_DEFAULT_FORMAT_VERSION = PART_METADATA_FORMAT_VERSION_OLD;
// Increment any time a new format version is added
const PartMetadataFormatVersion PART_METADATA_MAX_FORMAT_VERSION = PART_METADATA_FORMAT_VERSION_INITIAL;

class ReadBuffer;
class WriteBuffer;

class PartMetadataJSON
{
public:
    PartMetadataJSON() = default;
    PartMetadataJSON(Poco::JSON::Object::Ptr json_);

    void readJSON(ReadBuffer & in);
    void writeJSON(WriteBuffer & out) const;

    PartMetadataFormatVersion metadataFormatVersion() const;

    // Get the creationTime for this metadata
    // Requires metadata version >= PART_METADATA_FORMAT_VERSION_INITIAL, otherwise an exception is thrown
    //
    // creationTime represents the time at which merging logic was last applied for the data in this part
    // merging logic occurs when a part is inserted, or merged with other parts.
    // merging logic is not necessarily run when a mutation or delete occurs, nor when it is fetched from another replica or attached.
    time_t creationTime() const;
    // Get the creationTime for this metadata, or a default if metadata version is too low
    time_t creationTime(time_t fallback) const;

private:
    Poco::JSON::Object::Ptr json;
};

}
