#pragma once

#include "config.h"

#if USE_AVRO

#include <Storages/KeyDescription.h>
#include <Core/Field.h>

#include <cstdint>
#include <variant>

namespace Iceberg
{

struct ManifestFileContentImpl;

enum class ManifestEntryStatus : uint8_t
{
    EXISTING = 0,
    ADDED = 1,
    DELETED = 2,
};

enum class FileContentType : uint8_t
{
    DATA = 0,
    POSITION_DELETES = 1,
    EQUALITY_DELETES = 2,
};

struct DataFileEntry
{
    String file_name;
};

using FileEntry = std::variant<DataFileEntry>; // In the future we will add PositionalDeleteFileEntry and EqualityDeleteFileEntry here

/// Description of Data file in manifest file
struct ManifestFileEntry
{
    ManifestEntryStatus status;
    Int64 added_sequence_number;

    FileEntry file;
    DB::Row partition_key_value;
};

class ManifestFileContent
{
public:
    explicit ManifestFileContent(std::unique_ptr<ManifestFileContentImpl> impl_);

    const std::vector<ManifestFileEntry> & getFiles() const;
    Int32 getSchemaId() const;

    bool hasPartitionKey() const;
    const DB::KeyDescription & getPartitionKeyDescription() const;
    const std::vector<Int32> & getPartitionKeyColumnIDs() const;
private:
    std::unique_ptr<ManifestFileContentImpl> impl;
};


/// Once manifest file is constructed. It's unchangable.
using ManifestFilePtr = std::shared_ptr<const ManifestFileContent>;
using ManifestFilesStorage = std::map<String, ManifestFilePtr>;

}

#endif
